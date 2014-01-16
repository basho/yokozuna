%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(yz_entropy_mgr).
-compile(export_all).
-behaviour(gen_server).
-include("yokozuna.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {mode :: exchange_mode(),
                trees :: trees(),
                %% `kv_trees' is just local cache of KV Tree Pids, no
                %% need to poke them
                kv_trees :: trees(),
                tree_queue :: trees(),
                locks :: [{pid(),reference()}],
                build_tokens = 0 :: non_neg_integer(),
                exchange_queue :: [exchange()],
                exchanges :: [{p(),reference(), pid()}]}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Acquire an exchange concurrency lock if available, and associate
%%      the lock with the calling process.
-spec get_lock(term()) -> ok | max_concurrency.
get_lock(Type) ->
    get_lock(Type, self()).

-spec get_lock(term(), pid()) -> ok | max_concurrency.
get_lock(Type, Pid) ->
    gen_server:call(?MODULE, {get_lock, Type, Pid}, infinity).

-spec get_tree(p()) -> {ok, tree()} | not_registered.
get_tree(Index) ->
    %% NOTE: This is called by yz_kv:get_tree which is running on KV
    %%       vnode process.  Think about putting tree register in ETS
    %%       table and making it public for read to avoid blocking
    %%       vnode when entryopy mgr is backed up.
    gen_server:call(?MODULE, {get_tree, Index}, infinity).

%% @doc Used by {@link yz_index_hashtree} to requeue a poke on build
%%      failure.
-spec requeue_poke(p()) -> ok.
requeue_poke(Index) ->
    gen_server:cast(?MODULE, {requeue_poke, Index}).

%% @doc Used by {@link yz_exchange_fsm} to inform the entropy manager
%%      about the status of an exchange (ie. completed without issue,
%%      failed, etc)
-spec exchange_status(p(), {p(), n()}, term()) -> ok.
exchange_status(Index, IndexN, Status) ->
    gen_server:cast(?MODULE, {exchange_status, self(), Index, IndexN, Status}).

%% @doc Returns true of AAE is enabled, false otherwise.
-spec enabled() -> boolean().
enabled() ->
    riak_kv_entropy_manager:enabled().

%% @doc Set AAE to either `automatic' or `manual' mode. In automatic mode, the
%%      entropy manager triggers all necessary hashtree exchanges. In manual
%%      mode, exchanges must be triggered using {@link manual_exchange/1}.
%%      Regardless of exchange mode, the entropy manager will always ensure
%%      local hashtrees are built and rebuilt as necessary.
-spec set_mode(automatic | manual) -> ok.
set_mode(Mode=automatic) ->
    ok = gen_server:call(?MODULE, {set_mode, Mode}, infinity);
set_mode(Mode=manual) ->
    ok = gen_server:call(?MODULE, {set_mode, Mode}, infinity).

%% NOTE: Yokozuna only runs when KV AAE runs, but this API is needed
%% so that the Yokozuna hashtrees may be stopped.
disable() ->
    gen_server:call(?MODULE, disable, infinity).

%% @doc Manually trigger hashtree exchanges.
%%
%%      -- If a partition is provided, trigger exchanges between Yokozuna
%%         and KV for all preflists stored by the partition.
%%
%%      -- If both a partition and preflist are provided, trigger
%%         exchange between Yokozuna and KV for that index/preflist.
-spec manual_exchange(p() | {p(), {p(), n()}}) -> ok.
manual_exchange(Exchange) ->
    gen_server:call(?MODULE, {manual_exchange, Exchange}, infinity).

%% @doc Stop the exchange currently executing for `Index', if there
%%      is one.
-spec cancel_exchange(p()) -> ok | undefined.
cancel_exchange(Index) ->
    gen_server:call(?MODULE, {cancel_exchange, Index}, infinity).

%% @doc Stop all currently executing exchanges.
-spec cancel_exchanges() -> [p()].
cancel_exchanges() ->
    gen_server:call(?MODULE, cancel_exchanges, infinity).

%% @doc Clear all the trees.
-spec clear_trees() -> ok.
clear_trees() ->
    gen_server:cast(?MODULE, clear_trees).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    Trees = get_trees_from_sup(),
    schedule_tick(),
    {_, Opts} = settings(),
    Mode = case proplists:is_defined(manual, Opts) of
               true -> manual;
               false -> automatic
           end,
    S = #state{mode=Mode,
               trees=Trees,
               kv_trees=[],
               tree_queue=[],
               locks=[],
               exchanges=[],
               exchange_queue=[]},
    S2 = reset_build_tokens(S),
    schedule_reset_build_tokens(),
    {ok, S2}.

handle_call({get_lock, Type, Pid}, _From, S) ->
    {Reply, S2} = do_get_lock(Type, Pid, S),
    {reply, Reply, S2};

handle_call({get_tree, Index}, _From, S) ->
    Resp = get_tree(?YZ_SVC_NAME, Index, S),
    {reply, Resp, S};

handle_call({manual_exchange, Exchange}, _From, S) ->
    S2 = enqueue_exchange(Exchange, S),
    {reply, ok, S2};

handle_call({cancel_exchange, Index}, _From, S) ->
    case lists:keyfind(Index, 1, S#state.exchanges) of
        false ->
            {reply, undefined, S};
        {Index, _Ref, Pid} ->
            exit(Pid, kill),
            {reply, ok, S}
    end;

handle_call(cancel_exchanges, _From, S=#state{exchanges=Exchanges}) ->
    Indices = clear_all_exchanges(Exchanges),
    {reply, Indices, S};

handle_call(disable, _From, S) ->
    [yz_index_hashtree:stop(T) || {_,T} <- S#state.trees],
    {reply, ok, S};

handle_call({set_mode, Mode}, _From, S) ->
    S2 = S#state{mode=Mode},
    {reply, ok, S2};

handle_call(Request, From, S) ->
    lager:warning("Unexpected call: ~p from ~p", [Request, From]),
    {reply, unexpected_call, S}.

handle_cast({requeue_poke, Index}, S) ->
    S2 = requeue_poke(Index, S),
    {noreply, S2};

handle_cast({exchange_status, Pid, Index, {StartIdx, N}, Status}, S) ->
    S2 = do_exchange_status(Pid, Index, {StartIdx, N}, Status, S),
    {noreply, S2};

handle_cast(clear_trees, S) ->
    clear_all_exchanges(S#state.exchanges),
    clear_all_trees(S#state.trees),
    {noreply, S};

handle_cast(_Msg, S) ->
    lager:warning("Unexpected cast: ~p", [_Msg]),
    {noreply, S}.

handle_info(tick, S) ->
    S2 = maybe_tick(S),
    {noreply, S2};

handle_info(reset_build_tokens, S) ->
    S2 = reset_build_tokens(S),
    schedule_reset_build_tokens(),
    {noreply, S2};

handle_info({{hashtree_pid, Index}, Reply}, S) ->
    %% Reply from KV VNode, add Tree's `Pid' to locally cached list of
    %% KV Tree Pids.
    case Reply of
        {ok, Pid} when is_pid(Pid) ->
            case get_tree(riak_kv, Index, S) of
                {ok, Pid} ->
                    {noreply, S};
                _ ->
                    %% Either the Pid has changed or nothing was
                    %% registered for the index. In any case add the
                    %% Pid.
                    monitor(process, Pid),
                    KVTrees1 = S#state.kv_trees,
                    KVTrees2 = orddict:store(Index, Pid, KVTrees1),
                    S2 = S#state{kv_trees=KVTrees2},
                    {noreply, S2}
            end;
        _ ->
            %% The Tree doesn't exist on the KV side.
            {noreply, S}
    end;
handle_info({'DOWN', Ref, _, Obj, Status}, S) ->
    %% NOTE: The down msg could be for exchange FSM or tree
    S2 = maybe_release_lock(Ref, S),
    S3 = maybe_clear_exchange(Ref, Status, S2),
    S4 = maybe_clear_registered_tree(Obj, S3),
    {noreply, S4};

handle_info(_Msg, S) ->
    lager:warning("Unexpected msg: ~p", [_Msg]),
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec clear_all_exchanges(list()) -> [p()].
clear_all_exchanges(Exchanges) ->
    [begin
         exit(Pid, kill),
         Index
     end || {Index, _Ref, Pid} <- Exchanges].

clear_all_trees(Trees) ->
    [yz_index_hashtree:clear(TPid) || {_, TPid} <- Trees].

schedule_reset_build_tokens() ->
    {_, Reset} = ?YZ_ENTROPY_BUILD_LIMIT,
    erlang:send_after(Reset, self(), reset_build_tokens).

reset_build_tokens(S) ->
    {Tokens, _} = ?YZ_ENTROPY_BUILD_LIMIT,
    S#state{build_tokens=Tokens}.

-spec settings() -> {boolean(), proplists:proplist()}.
settings() ->
    case app_helper:get_env(?YZ_APP_NAME, anti_entropy,
            app_helper:get_env(riak_kv, anti_entropy, {off, []})
        ) of
        {on, Opts} ->
            {true, Opts};
        {off, Opts} ->
            {false, Opts};
        X ->
            lager:warning("Invalid setting for riak_kv/anti_entropy: ~p", [X]),
            application:set_env(?YZ_APP_NAME, anti_entropy, {off, []}),
            application:set_env(riak_kv, anti_entropy, {off, []}),
            {false, []}
    end.

%% @private
-spec get_tree(atom(), p(), state()) -> {ok, tree()} | not_registered.
get_tree(Service, Index, S) ->
    Trees = case Service of
                riak_kv -> S#state.kv_trees;
                yokozuna -> S#state.trees
            end,
    case orddict:find(Index, Trees) of
        {ok, Tree} -> {ok, Tree};
        error -> not_registered
    end.

%% @private
%%
%% @doc Generate a list of all the trees currently active.  It enables
%%      the entropy manager to rediscover the trees in the case of a
%%      crash.
-spec get_trees_from_sup() -> trees().
get_trees_from_sup() ->
    Trees = yz_index_hashtree_sup:trees(),
    lists:foldl(fun get_index/2, [], Trees).

%% @private
%%
%% @doc Get the index for the `Child' make a pair and add to `Trees'.
-spec get_index(tree(), trees()) -> trees().
get_index(Tree, Trees) ->
    case yz_index_hashtree:get_index(Tree) of
        {error, _} -> Trees;
        Index -> [{Index,Tree}|Trees]
    end.

reload_hashtrees(Ring, S) ->
    reload_hashtrees(enabled(), Ring, S).

-spec reload_hashtrees(boolean(), ring(), state()) -> state().
reload_hashtrees(true, Ring, S=#state{mode=Mode, trees=Trees}) ->
    Indices = riak_core_ring:my_indices(Ring),
    Existing = orddict:from_list(Trees),

    MissingIdx = [Idx || Idx <- Indices, not orddict:is_key(Idx, Existing)],
    L = lists:foldl(fun(Idx, NewTrees) ->
                            RPs = riak_kv_util:responsible_preflists(Idx),
                            {ok, Tree} = yz_index_hashtree:start(Idx, RPs),
                            [{Idx,Tree}|NewTrees]
                    end, [], MissingIdx),
    Trees2 = orddict:from_list(Trees ++ L),

    Moved = [E || E={Idx,_} <- Trees2, not lists:member(Idx, Indices)],
    Trees3 = remove_trees(Trees2, Moved),

    S2 = S#state{trees=Trees3},
    S3 = lists:foldl(fun({Idx,Pid}, SAcc) ->
                             monitor(process, Pid),
                             case Mode of
                                 manual -> SAcc;
                                 automatic -> enqueue_exchange(Idx, SAcc)
                             end
                     end, S2, L),
    S4 = reload_kv_trees(Ring, S3),
    S4;
reload_hashtrees(false, _, S) ->
    S.

%% @private
reload_kv_trees(Ring, S) ->
    KVTrees = S#state.kv_trees,
    Indices = riak_core_ring:my_indices(Ring),
    Existing = dict:from_list(KVTrees),
    MissingIdx = [Idx || Idx <- Indices,
                         not dict:is_key(Idx, Existing)],
    %% Make async request for tree pid, add to `kv_trees' in `handle_info'.
    [riak_kv_vnode:request_hashtree_pid(Idx) || Idx <- MissingIdx],
    S.


%% @private
%%
%% @doc Remove trees from `Trees' and destroy the hashtrees.
-spec remove_trees(trees(), trees()) -> trees().
remove_trees(Trees, ToRemove) ->
    F = fun({Idx, Tree}, TreesAcc) ->
                yz_index_hashtree:destroy(Tree),
                orddict:erase(Idx, TreesAcc)
        end,
    lists:foldl(F, Trees, ToRemove).

-spec do_get_lock(term(), pid(), state()) ->
                         {ok | max_concurrency | build_limit_reached, state()}.
do_get_lock(Type, Pid, S=#state{locks=Locks}) ->
    case length(Locks) >= ?YZ_ENTROPY_CONCURRENCY of
        true ->
            {max_concurrency, S};
        false ->
            case check_lock_type(Type, S) of
                {ok, S2} ->
                    Ref = monitor(process, Pid),
                    S3 = S2#state{locks=[{Pid,Ref}|Locks]},
                    {ok, S3};
                Error ->
                    {Error, S}
            end
    end.


-spec check_lock_type(term(), state()) -> {ok, state()} | build_limit_reached.
check_lock_type(build, S=#state{build_tokens=Tokens}) ->
    if Tokens > 0 ->
            {ok, S#state{build_tokens=Tokens-1}};
       true ->
            build_limit_reached
    end;
check_lock_type(_Type, S) ->
    {ok, S}.

-spec maybe_release_lock(reference(), state()) -> state().
maybe_release_lock(Ref, S) ->
    Locks = lists:keydelete(Ref, 2, S#state.locks),
    S#state{locks=Locks}.

-spec maybe_clear_exchange(reference(), term(), state()) -> state().
maybe_clear_exchange(Ref, Status, S) ->
    case lists:keytake(Ref, 2, S#state.exchanges) of
        false ->
            S;
        {value, {Idx,Ref,_Pid}, Exchanges} ->
            lager:debug("Untracking exchange: ~p :: ~p", [Idx, Status]),
            S#state{exchanges=Exchanges}
    end.

%% @private
%%
%% @doc If the `Pid' matches a cached copy for either a Yokozuna or KV
%% trees then remove it from the list.
-spec maybe_clear_registered_tree(pid(), state()) -> state().
maybe_clear_registered_tree(Pid, S) when is_pid(Pid) ->
    YZTrees = lists:keydelete(Pid, 2, S#state.trees),
    KVTrees = lists:keydelete(Pid, 2, S#state.kv_trees),
    S#state{trees=YZTrees, kv_trees=KVTrees};
maybe_clear_registered_tree(_, S) ->
    S.

-spec next_tree(state()) -> {pid(), state()} | {none, state()}.
next_tree(S=#state{tree_queue=Queue, trees=Trees}) ->
    More = fun() -> Trees end,
    case yz_misc:queue_pop(Queue, More) of
        {{_,Pid}, Rest} ->
            S2 = S#state{tree_queue=Rest},
            {Pid, S2};
        empty ->
            {none, S}
    end.

-spec schedule_tick() -> reference().
schedule_tick() ->
    erlang:send_after(?YZ_ENTROPY_TICK, ?MODULE, tick).

maybe_tick(S) ->
    case enabled() of
        true ->
            case riak_core_capability:get({riak_kv, anti_entropy}, disabled) of
                disabled -> S2 = S;
                enabled_v1 -> S2 = tick(S)
            end;
        false ->
            %% Ensure we do not have any running index_hashtrees, which can
            %% happen when disabling anti-entropy on a live system.
            [yz_index_hashtree:stop(T) || {_,T} <- S#state.trees],
            S2 = S
    end,
    schedule_tick(),
    S2.

-spec tick(state()) -> state().
tick(S) ->
    Ring = yz_misc:get_ring(transformed),
    S2 = reload_hashtrees(Ring, S),
    S3 = lists:foldl(fun(_,SAcc) ->
                             maybe_poke_tree(SAcc)
                     end, S2, lists:seq(1,10)),
    maybe_exchange(Ring, S3).

-spec maybe_poke_tree(state()) -> state().
maybe_poke_tree(S) ->
    case next_tree(S) of
        {none, S2} ->
            S2;
        {Tree, S2} ->
            yz_index_hashtree:poke(Tree),
            S2
    end.

%%%===================================================================
%%% Exchanging
%%%===================================================================

-spec do_exchange_status(pid(), p(), {p(), n()}, any(), state()) -> state().
do_exchange_status(_Pid, Index, {StartIdx, N}, Status, S) ->
    case Status of
        ok ->
            lager:debug("Finished exhcange for partition ~p of preflist ~p",
                        [Index, {StartIdx, N}]),
            S;
        _ ->
            lager:debug("Requeue exhcange for partition ~p of preflist ~p "
                        "for reason ~p",
                        [Index, {StartIdx, N}, Status]),
            requeue_exchange(Index, {StartIdx, N}, S)
    end.

-spec start_exchange(p(), {p(),n()}, ring(), state()) -> {any(), state()}.
start_exchange(Index, Preflist, Ring, S) ->
    IsOwner = riak_core_ring:index_owner(Ring, Index) == node(),
    PendingChange = is_pending_change(Ring, Index),

    %% NOTE: The riak_kv version of this function wraps this is a
    %% try/catch to guard against the case where a ring size change
    %% makes the index go way. I disagree with the use of try/catch as
    %% it could fail for other reaons. I believe this can be solved by
    %% checking if the index exists in the `maybe_exchange'
    %% function. But first would like to actually test Yokozuna AAE +
    %% ring resizing to see what happens.
    case {IsOwner, PendingChange} of
        {false, _} ->
            {not_responsible, S};
        {true, false} ->
            %% NOTE: even though ownership change is checked this
            %%       could still return `{error,wrong_node}' because
            %%       ownership change could have started since the
            %%       check.
            YZ = get_tree(?YZ_SVC_NAME, Index, S),
            KV = get_tree(riak_kv, Index, S),

            case {YZ, KV} of
                {{ok, YZTree}, {ok, KVTree}} ->
                    case yz_exchange_fsm:start(Index, Preflist, YZTree,
                                               KVTree, self()) of
                        {ok, FsmPid} ->
                            Ref = monitor(process, FsmPid),
                            E = S#state.exchanges,
                            %% TODO: add timestamp so we know how long ago
                            %%       exchange was started
                            {ok, S#state{exchanges=[{Index,Ref,FsmPid}|E]}};
                        {error, Reason} ->
                            {Reason, S}
                    end;
                {{ok,_}, NotOK} ->
                    {{kv_tree, NotOK}, S};
                {NotOK, {ok,_}} ->
                    {{yz_tree, NotOK}, S}
            end;
        {true, true} ->
            {pending_ownership_change, S}
    end.

%% @doc Predicate to determine if the `Partition' is currently under
%%      ownership change according to `Ring'.
-spec is_pending_change(ring(), p()) -> boolean().
is_pending_change(Ring, Partition) ->
    case lists:keyfind(Partition, 1, riak_core_ring:pending_changes(Ring)) of
        false -> false;
        _ -> true
    end.

-spec all_pairwise_exchanges(p(), ring()) -> [exchange()].
all_pairwise_exchanges(Index, Ring) ->
    RPs = riak_kv_util:responsible_preflists(Index, Ring),
    [{Index, {StartIdx, N}} || {StartIdx, N} <- RPs].

-spec all_exchanges(ring(), trees()) -> [exchange()].
all_exchanges(Ring, Trees) ->
    Indices = orddict:fetch_keys(Trees),
    lists:flatmap(fun(Index) ->
                          all_pairwise_exchanges(Index, Ring)
                  end, Indices).

-spec enqueue_exchange(p() | {p(), {p(),n()}}, state()) -> state().
enqueue_exchange(E={_Index,_IndexN}, S) ->
    case verify_exchange(E) of
        true ->
            enqueue_exchanges([E], S);
        false ->
            S
    end;

enqueue_exchange(Index, S) ->
    Ring = yz_misc:get_ring(transformed),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    enqueue_exchanges(Exchanges, S).

enqueue_exchanges(Exchanges, S) ->
    EQ = S#state.exchange_queue ++ Exchanges,
    S#state{exchange_queue=EQ}.

-spec verify_exchange({p(), n()}) -> boolean().
verify_exchange(E={Index,_N}) ->
    Ring = yz_misc:get_ring(transformed),
    ValidExchanges = all_pairwise_exchanges(Index, Ring),
    lists:member(E, ValidExchanges).

-spec already_exchanging(p(), state()) -> boolean().
already_exchanging(Index, #state{exchanges=E}) ->
    case lists:keyfind(Index, 1, E) of
        false -> false;
        {Index,_,_} -> true
    end.

-spec maybe_exchange(ring(), state()) -> state().
maybe_exchange(Ring, S) ->
    case next_exchange(Ring, S) of
        {none, S2} ->
            S2;
        {NextExchange, S2} ->
            {Index, IndexN} = NextExchange,
            case already_exchanging(Index, S) of
                true ->
                    requeue_exchange(Index, IndexN, S2);
                false ->
                    case start_exchange(Index, IndexN, Ring, S2) of
                        {ok, S3} -> S3;
                        {_, S3} -> S3
                    end
            end
    end.

-spec init_next_exchange(state()) -> state().
init_next_exchange(S) ->
    Trees = S#state.trees,
    Exchanges = all_exchanges(yz_misc:get_ring(transformed), Trees),
    S#state{exchange_queue=Exchanges}.

-spec next_exchange(ring(), state()) -> {exchange(), state()} | {none, state()}.
next_exchange(Ring, S=#state{exchange_queue=Exchanges}) ->
    More = fun() ->
                   case S#state.mode of
                       automatic -> all_exchanges(Ring, S#state.trees);
                       manual -> []
                   end
           end,
    case yz_misc:queue_pop(Exchanges, More) of
        {Exchange, Rest} ->
            S2 = S#state{exchange_queue=Rest},
            {Exchange, S2};
        empty ->
            {none, S}
    end.

-spec requeue_poke(p(), state()) -> state().
requeue_poke(Index, S=#state{trees=Trees}) ->
    case orddict:find(Index, Trees) of
        {ok, Tree} ->
            Queue = S#state.tree_queue ++ [{Index,Tree}],
            S#state{tree_queue=Queue};
        _ ->
            S
    end.

-spec requeue_exchange(p(), {p(), n()}, state()) -> state().
requeue_exchange(Index, {StartIdx, N}, S) ->
    Exchange = {Index, {StartIdx, N}},
    case lists:member(Exchange, S#state.exchange_queue) of
        true ->
            S;
        false ->
            Exchanges = S#state.exchange_queue ++ [Exchange],
            S#state{exchange_queue=Exchanges}
    end.
