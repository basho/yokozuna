-module(yz_entropy_mgr).
-compile(export_all).
-behaviour(gen_server).
-include("yokozuna.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {trees :: trees(),
                tree_queue :: trees(),
                locks :: [{pid(),reference()}],
                exchange_queue :: [exchange()],
                exchanges :: [{p(),reference()}]}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_lock(Type) ->
    get_lock(Type, self()).

get_lock(Type, Pid) ->
    gen_server:call(?MODULE, {get_lock, Type, Pid}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    Trees = get_trees_from_sup(),
    schedule_tick(),
    S = #state{trees=Trees,
               tree_queue=[],
               locks=[],
               exchanges=[],
               exchange_queue=[]},
    {ok, S}.

handle_call({get_lock, Type, Pid}, _From, S) ->
    {Reply, S2} = do_get_lock(Type, Pid, S),
    {reply, Reply, S2};

handle_call(Request, From, S) ->
    lager:warning("Unexpected call: ~p from ~p", [Request, From]),
    {reply, unexpected_call, S}.

handle_cast({requeue_poke, Index}, S) ->
    S2 = requeue_poke(Index, S),
    {noreply, S2};

handle_cast({exchange_status, Pid, Index, {StartIdx, N}, Reply}, S) ->
    S2 = do_exchange_status(Pid, Index, {StartIdx, N}, Reply, S),
    {noreply, S2};

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(tick, S) ->
    S2 = reload_hashtrees(S),
    S3 = tick(S2),
    schedule_tick(),
    {noreply, S3};

handle_info({'DOWN', Ref, _, Obj, _}, S) ->
    %% NOTE: The down msg could be for exchange FSM or tree
    S2 = maybe_release_lock(Ref, S),
    S3 = maybe_clear_exchange(Ref, S2),
    S4 = maybe_clear_registered_tree(Obj, S3),
    {noreply, S4};

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%%
%% @doc Generate a list of all the trees currently active.  It enables
%%      the entropy manager to rediscover the trees in the case of a
%%      crash.
-spec get_trees_from_sup() -> trees().
get_trees_from_sup() ->
    Children = yz_index_hashtree_sup:children(),
    lists:foldl(fun get_index/2, [], Children).

%% @private
%%
%% @doc Get the index for the `Child' make a pair and add to `Trees'.
-spec get_index(pid(), trees()) -> trees().
get_index(Child, Trees) ->
    case yz_index_hashtree:index(Child) of
        {error, _} -> Trees;
        Index -> [{Index,Child}|Trees]
    end.

-spec reload_hashtrees(state()) -> state().
reload_hashtrees(S=#state{trees=Trees}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    Existing = dict:from_list(Trees),

    MissingIdx = [Idx || Idx <- Indices, not dict:is_key(Idx, Existing)],
    L = [yz_index_hashtree:start(Idx) || Idx <- MissingIdx],
    Trees2 = orddict:from_list(Trees ++ L),

    Moved = [Idx || {Idx,_} <- Trees2, not lists:member(Idx, Indices)],
    Trees3 = remove_trees(Trees2, Moved),

    S2 = S#state{trees=Trees3},
    S3 = lists:foldl(fun({Idx,Pid}, SAcc) ->
                                 monitor(process, Pid),
                                 add_index_exchanges(Idx, SAcc)
                         end, S2, L),
    S3.

%% @private
%%
%% @doc Remove trees from `Trees' and remove the hashtrees.
-spec remove_trees(trees(), [p()]) -> trees().
remove_trees(Trees, ToRemove) ->
    F = fun(Idx,TreesAcc) -> orddict:erase(Idx, TreesAcc) end,
    [yz_index_hashtree:remove(Idx) || Idx <- ToRemove],
    lists:foldl(F, Trees, ToRemove).

%% NOTES: Exchange FSM grabs the lock and thus is monitored
do_get_lock(_Type, Pid, S=#state{locks=Locks}) ->
    case length(Locks) >= ?YZ_HASH_EXCHANGE_CONCURRENCY of
        true ->
            {max_concurrency, S};
        false ->
            Ref = monitor(process, Pid),
            S2 = S#state{locks=[{Pid,Ref}|Locks]},
            {ok, S2}
    end.

maybe_release_lock(Ref, S) ->
    Locks = lists:keydelete(Ref, 2, S#state.locks),
    S#state{locks=Locks}.

maybe_clear_exchange(Ref, S) ->
    case lists:keyfind(Ref, 2, S#state.exchanges) of
        false ->
            ok;
        {Idx,Ref} ->
            lager:info("Untracking exchange: ~p", [Idx])
    end,
    Exchanges = lists:keydelete(Ref, 2, S#state.exchanges),
    S#state{exchanges=Exchanges}.

maybe_clear_registered_tree(Pid, S) when is_pid(Pid) ->
    Trees = lists:keydelete(Pid, 2, S#state.trees),
    S#state{trees=Trees};
maybe_clear_registered_tree(_, S) ->
    S.

next_tree(#state{trees=[]}) ->
    [];
next_tree(S=#state{tree_queue=Queue, trees=Trees}) ->
    More = fun() -> Trees end,
    {[{_,Pid}], Rest} = yz_misc:queue_pop(Queue, 1, More),
    S2 = S#state{tree_queue=Rest},
    {Pid, S2}.

schedule_tick() ->
    erlang:send_after(?YZ_ENTROPY_TICK, ?MODULE, tick).

tick(S) ->
    S2 = lists:foldl(fun(_,S) ->
                                 maybe_poke_tree(S)
                         end, S, lists:seq(1,10)),
    maybe_exchange(S2).

maybe_poke_tree(S=#state{trees=[]}) ->
    S;
maybe_poke_tree(S) ->
    {Tree, S2} = next_tree(S),
    %% TODO: replace with yz_index_hashtree:poke/1
    gen_server:cast(Tree, poke),
    S2.

%%%===================================================================
%%% Exchanging
%%%===================================================================

do_exchange_status(_Pid, Index, {StartIdx, N}, Reply, S) ->
    case Reply of
        ok -> S;
        _ -> requeue_exchange(Index, {StartIdx, N}, S)
    end.

-spec start_exchange(p(), {p(),n()}, state()) ->
                            {ok, state()} | {any(), state()}.
start_exchange(Index, {StartIdx, N}, S) ->
    case exchange_fsm:start(Index, StartIdx, N) of
        {ok, FsmPid} ->
            %% Make this happen automatically as part of init in exchange_fsm
            lager:info("Start exchange of partition ~p for preflist {~p, ~p}",
                       [Index, StartIdx, N]),
            %% exchange_fsm handles locking: tries to get concurrency lock, then index_ht lock
            Tree = orddict:fetch(Index, S#state.trees),
            exchange_fsm:start_exchange(FsmPid, Tree, self()),
            %% Do we want to monitor exchange FSMs?
            %% Do we want to track exchange FSMs?
            Ref = monitor(process, FsmPid),
            E = S#state.exchanges,
            {ok, S#state{exchanges=[{Index,Ref}|E]}};
        {error, Reason} ->
            {Reason, S}
    end.

%% Exchanges between yz and KV are RPs
-spec all_pairwise_exchanges(p(), ring()) -> [exchange()].
all_pairwise_exchanges(Index, Ring) ->
    RPs = riak_kv_vnode:responsible_preflists(Index, Ring),
    [{Index, {StartIdx, N}} || {StartIdx, N} <- RPs].

-spec all_exchanges(ring(), trees()) -> [exchange()].
all_exchanges(Ring, Trees) ->
    Indices = orddict:fetch_keys(Trees),
    lists:flatmap(fun(Index) ->
                          all_pairwise_exchanges(Index, Ring)
                  end, Indices).

-spec add_index_exchanges(p(), state()) -> state().
add_index_exchanges(Index, S) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    EQ = S#state.exchange_queue ++ Exchanges,
    S#state{exchange_queue=EQ}.

already_exchanging(Index, #state{exchanges=E}) ->
    case lists:keyfind(Index, 1, E) of
        false ->
            false;
        {Index,_} ->
            true
    end.

maybe_exchange(S) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case next_exchange(Ring, S) of
        {none, S2} ->
            S2;
        {NextExchange, S2} ->
            {Index, {StartIdx, N}} = NextExchange,
            case already_exchanging(Index, S) of
                true ->
                    requeue_exchange(Index, {StartIdx, N}, S2);
                false ->
                    case start_exchange(Index, {StartIdx, N}, S2) of
                        {ok, S3} -> S3;
                        {_Reason, S3} -> S3
                    end
            end
    end.

init_next_exchange(S) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Trees = S#state.trees,
    Exchanges = all_exchanges(Ring, Trees),
    S#state{exchange_queue=Exchanges}.

next_exchange(_Ring, S=#state{exchange_queue=[], trees=[]}) ->
    {none, S};
next_exchange(Ring, S=#state{exchange_queue=Exchanges}) ->
    More = fun() ->
                   all_exchanges(Ring, S#state.trees)
           end,
    {[Exchange], Rest} = yz_misc:queue_pop(Exchanges, 1, More),
    S2 = S#state{exchange_queue=Rest},
    {Exchange, S2}.

-spec requeue_poke(p(), state()) -> state().
requeue_poke(Index, S=#state{trees=Trees}) ->
    case orddict:find(Index, Trees) of
        {ok, Tree} ->
            Queue = S#state.tree_queue ++ [{Index,Tree}],
            S#state{tree_queue=Queue};
        _ ->
            S
    end.

requeue_exchange(Index, {StartIdx, N}, S) ->
    Exchange = {Index, {StartIdx, N}},
    case lists:member(Exchange, S#state.exchange_queue) of
        true ->
            S;
        false ->
            lager:info("Requeue exhcange for partition ~p of preflist ~p",
                       [{Index, {StartIdx, N}}]),
            Exchanges = S#state.exchange_queue ++ [Exchange],
            S#state{exchange_queue=Exchanges}
    end.
