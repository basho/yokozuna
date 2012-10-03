-module(yz_entropy_mgr).
-compile(export_all).
-behaviour(gen_server).
-include("yokozuna.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {trees :: trees(),
                tree_queue :: trees(),
                locks,
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
    State = #state{trees=Trees,
                   tree_queue=[],
                   locks=[],
                   exchanges=[],
                   exchange_queue=[]},
    {ok, State}.

handle_call({get_lock, Type, Pid}, _From, State) ->
    {Reply, State2} = do_get_lock(Type, Pid, State),
    {reply, Reply, State2};
handle_call(Request, From, State) ->
    lager:warning("Unexpected message: ~p from ~p", [Request, From]),
    {reply, ok, State}.

handle_cast({requeue_poke, Index}, State) ->
    State2 = requeue_poke(Index, State),
    {noreply, State2};
handle_cast({exchange_status, Pid, LocalVN, RemoteVN, IndexN, Reply}, State) ->
    State2 = do_exchange_status(Pid, LocalVN, RemoteVN, IndexN, Reply, State),
    {noreply, State2};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    State2 = reload_hashtrees(State),
    State3 = tick(State2),
    {noreply, State3};
handle_info({'DOWN', Ref, _, Obj, _}, State) ->
    State2 = maybe_release_lock(Ref, State),
    State3 = maybe_clear_exchange(Ref, State2),
    State4 = maybe_clear_registered_tree(Obj, State3),
    {noreply, State4};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%%
%% @doc Generate a list of all the trees currently active.
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
reload_hashtrees(State=#state{trees=Trees}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    Existing = dict:from_list(Trees),

    MissingIdx = [Idx || Idx <- Indices, not dict:is_key(Idx, Existing)],
    L = [yz_index_hashtree:start(Idx) || Idx <- MissingIdx],
    Trees2 = orddict:from_list(Trees ++ L),

    Moved = [Idx || {Idx,_} <- Trees2, not lists:member(Idx, Indices)],
    Trees3 = remove_trees(Trees2, Moved),

    State2 = State#state{trees=Trees3},
    State3 = lists:foldl(fun({Idx,Pid}, StateAcc) ->
                                 monitor(process, Pid),
                                 add_index_exchanges(Idx, StateAcc)
                         end, State2, L),
    State3.

%% @private
%%
%% @doc Remove trees from `Trees' and remove the hashtrees.
-spec remove_trees(trees(), [p()]) -> trees().
remove_trees(Trees, ToRemove) ->
    F = fun(Idx,TreesAcc) -> orddict:erase(Idx, TreesAcc) end,
    [yz_index_hashtree:remove(Idx) || Idx <- ToRemove],
    lists:foldl(F, Trees, ToRemove).

do_get_lock(_Type, Pid, State=#state{locks=Locks}) ->
    case length(Locks) >= ?YZ_HASH_EXCHANGE_CONCURRENCY of
        true ->
            {max_concurrency, State};
        false ->
            Ref = monitor(process, Pid),
            State2 = State#state{locks=[{Pid,Ref}|Locks]},
            {ok, State2}
    end.

maybe_release_lock(Ref, State) ->
    Locks = lists:keydelete(Ref, 2, State#state.locks),
    State#state{locks=Locks}.

maybe_clear_exchange(Ref, State) ->
    case lists:keyfind(Ref, 2, State#state.exchanges) of
        false ->
            ok;
        {Idx,Ref} ->
            lager:info("Untracking exchange: ~p", [Idx])
    end,
    Exchanges = lists:keydelete(Ref, 2, State#state.exchanges),
    State#state{exchanges=Exchanges}.

maybe_clear_registered_tree(Pid, State) when is_pid(Pid) ->
    Trees = lists:keydelete(Pid, 2, State#state.trees),
    State#state{trees=Trees};
maybe_clear_registered_tree(_, State) ->
    State.

next_tree(#state{trees=[]}) ->
    [];
next_tree(State=#state{tree_queue=Queue, trees=Trees}) ->
    More = fun() -> Trees end,
    {[{_,Pid}], Rest} = yz_misc:queue_pop(Queue, 1, More),
    State2 = State#state{tree_queue=Rest},
    {Pid, State2}.

schedule_tick() ->
    erlang:send_after(?YZ_ENTROPY_TICK, ?MODULE, tick).

tick(State) ->
    State2 = lists:foldl(fun(_,S) ->
                                 maybe_poke_tree(S)
                         end, State, lists:seq(1,10)),
    State3 = maybe_exchange(State2),
    schedule_tick(),
    State3.

maybe_poke_tree(State=#state{trees=[]}) ->
    State;
maybe_poke_tree(State) ->
    {Tree, State2} = next_tree(State),
    gen_server:cast(Tree, poke),
    State2.

%%%===================================================================
%%% Exchanging
%%%===================================================================

do_exchange_status(_Pid, LocalVN, RemoteVN, IndexN, Reply, State) ->
    {LocalIdx, _} = LocalVN,
    {RemoteIdx, _} = RemoteVN,
    lager:info("S: ~p", [Reply]),
    case Reply of
        ok ->
            State;
        _ ->
            %% lager:info("Requeuing rate limited exchange"),
            State2 = requeue_exchange(LocalIdx, RemoteIdx, IndexN, State),
            State2
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
add_index_exchanges(Index, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Exchanges = all_pairwise_exchanges(Index, Ring),
    EQ = State#state.exchange_queue ++ Exchanges,
    State#state{exchange_queue=EQ}.

already_exchanging(Index, #state{exchanges=E}) ->
    case lists:keyfind(Index, 1, E) of
        false ->
            false;
        {Index,_} ->
            true
    end.

maybe_exchange(State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case next_exchange(Ring, State) of
        {none, State2} ->
            State2;
        {NextExchange, State2} ->
            {Index, {StartIdx, N}} = NextExchange,
            case already_exchanging(Index, State) of
                true ->
                    requeue_exchange(Index, StartIdx, N, State2);
                false ->
                    case start_exchange(Index, {StartIdx, N}, State2) of
                        {ok, State3} ->
                            State3;
                        {_Reason, State3} ->
                            State3
                    end
            end
    end.

init_next_exchange(S) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Trees = S#state.trees,
    Exchanges = all_exchanges(Ring, Trees),
    S#state{exchange_queue=Exchanges}.

next_exchange(_Ring, State=#state{exchange_queue=[], trees=[]}) ->
    {none, State};
next_exchange(Ring, S=#state{exchange_queue=Exchanges}) ->
    More = fun() ->
                   all_exchanges(Ring, S#state.trees)
           end,
    {[Exchange], Rest} = yz_misc:queue_pop(Exchanges, 1, More),
    S2 = S#state{exchange_queue=Rest},
    {Exchange, S2}.

requeue_poke(Index, State=#state{trees=Trees}) ->
    case orddict:find(Index, Trees) of
        {ok, Tree} ->
            Queue = State#state.tree_queue ++ [{Index,Tree}],
            State#state{tree_queue=Queue};
        _ ->
            State
    end.

requeue_exchange(Index, StartIdx, N, State) ->
    Exchange = {Index, {StartIdx, N}},
    case lists:member(Exchange, State#state.exchange_queue) of
        true ->
            State;
        false ->
            lager:info("Requeue exhcange for partition ~p of preflist ~p",
                       [{Index, {StartIdx, N}}]),
            Exchanges = State#state.exchange_queue ++ [Exchange],
            State#state{exchange_queue=Exchanges}
    end.
