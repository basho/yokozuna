-module(yz_exchange_fsm).
-behaviour(gen_fsm).
-include("yokozuna.hrl").
-compile(export_all).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-record(state, {index :: p(),
                index_n :: {p(),n()},
                yz_tree :: tree(),
                kv_tree :: tree(),
                built :: integer(),
                from :: any()}).

%% Per state transition timeout used by certain transitions
-define(DEFAULT_ACTION_TIMEOUT, 20000).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Initialize the exchange FSM to exchange between Yokozuna and
%%      KV for the `Preflist' replicas on `Index'.
-spec start(p(), {p(),n()}) -> {ok, pid()} | {error, any()}.
start(Index, Preflist) ->
    gen_fsm:start(?MODULE, [Index, Preflist], []).

%% @doc Start the exchange on `FSM' and send the result to `Pid'.
start_exchange(FSM, Pid) ->
    gen_fsm:send_event(FSM, {start_exchange, Pid}).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% step1: have Index and Preflist for exchange - pass to fsm
%%
%% step2: try to aquie lock from yz mgr and kv mgr
%%
%% step3: get pid of yz tree from mgr and kv tree from my
%%
%% step4: aquire lock from yz tree and kv tree
%%
%% step5: start exchange of Preflist between yz and kv tree
init([Index, Preflist]) ->
    S = #state{index=Index,
               index_n=Preflist,
               built=0},
    {ok, prepare_exchange, S}.

prepare_exchange({start_exchange, From}, S) ->
    monitor(process, From),
    prepare_exchange(start_exchange, S#state{from=From});

prepare_exchange(start_exchange, S) ->
    %% Get locks and pids of yokozuna and KV trees
    Index = S#state.index,
    case yz_entropy_mgr:get_lock(exchange) of
        max_concurrency ->
            maybe_reply(max_concurrency, S),
            %% TODO: russell said normal is not valid stop reason for FSM
            {stop, normal, S};
        ok ->
            %% TODO: check for not_registered
            YZTree = yz_entropy_mgr:get_tree(Index),
            case yz_index_hashtree:get_lock(YZTree, fsm) of
                ok ->
                    case entropy_manager:get_lock(yokozuna_exchange) of
                        max_concurrency ->
                            maybe_reply(max_concurrency, S),
                            {stop, normal, S};
                        ok ->
                            KVTree = entropy_manager:get_tree(Index),
                            S2 = S#state{yz_tree=YZTree, kv_tree=KVTree},
                            monitor(process, YZTree),
                            monitor(process, KVTree),
                            case index_hashtree:get_lock(KVTree, yz_fsm) of
                                ok ->
                                    update_trees(start_exchange, S2);
                                _ ->
                                    maybe_reply(already_locked, S)
                            end
                    end;
                _ ->
                    maybe_reply(already_locked, S),
                    {stop, normal, S}
            end
    end;

prepare_exchange(timeout, S) ->
    do_timeout(S).

update_trees(start_exchange, S=#state{yz_tree=YZTree,
                                      kv_tree=KVTree,
                                      index=Index,
                                      index_n=IndexN}) ->

    update_request(yz_index_hashtree, YZTree, Index, IndexN),
    update_request(index_hashtree, KVTree, Index, IndexN),
    next_state_with_timeout(update_trees, S);

update_trees(timeout, S) ->
    do_timeout(S);

update_trees({not_responsible, Index, IndexN}, S) ->
    lager:info("Index ~p does not cover preflist ~p", [Index, IndexN]),
    maybe_reply({not_responsible, Index, IndexN}, S),
    {stop, normal, S};

update_trees({tree_built, _, _}, S) ->
    Built = S#state.built + 1,
    case Built of
        2 ->
            lager:info("Moving to key exchange"),
            {next_state, key_exchange, S, 0};
        _ ->
            next_state_with_timeout(update_trees, S#state{built=Built})
    end.

key_exchange(timeout, S=#state{index=Index,
                               yz_tree=YZTree,
                               kv_tree=KVTree,
                               index_n=IndexN}) ->
    lager:info("Starting key exchange for partition ~p preflist ~p",
               [Index, IndexN]),

    Remote = fun(get_bucket, {L, B}) ->
                     exchange_bucket_kv(KVTree, IndexN, L, B);
                (key_hashes, Segment) ->
                     exchange_segment_kv(KVTree, IndexN, Segment)
             end,

    %% TODO: Do we want this to be sync? Do we want FSM to be able to timeout?
    %%       Perhaps this should be sync but we have timeout tick?
    {ok, RC} = riak:local_client(),
    AccFun = fun(KeyDiff, Acc) ->
                     lists:foreach(fun(Diff) ->
                                           read_repair_keydiff(RC, Diff)
                                   end, KeyDiff),
                     Acc
             end,
    index_hashtree:compare(IndexN, Remote, AccFun, YZTree),
    {stop, normal, S}.

read_repair_keydiff(RC, {_, KeyBin}) ->
    {Bucket, Key} = binary_to_term(KeyBin),
    lager:info("Anti-entropy forced read repair: ~p/~p", [Bucket, Key]),
    RC:get(Bucket, Key),
    ok.

exchange_bucket_kv(Tree, IndexN, Level, Bucket) ->
    index_hashtree:exchange_bucket(IndexN, Level, Bucket, Tree).

exchange_segment_kv(Tree, IndexN, Segment) ->
    index_hashtree:exchange_segment(IndexN, Segment, Tree).

handle_event(_Event, _StateName, State) ->
    {stop, badmsg, State}.

handle_sync_event(_Event, _From, _StateName, State) ->
    {stop, badmsg, State}.

handle_info({'DOWN', _, _, _, _}, _StateName, State) ->
    %% Either the entropy manager, local hashtree, or remote hashtree has
    %% exited. Stop exchange.
    {stop, normal, State};
handle_info(_Info, _StateName, State) ->
    {stop, badmsg, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

update_request(Module, Tree, Index, IndexN) ->
    as_event(fun() ->
                     case Module:update(IndexN, Tree) of
                         ok ->
                             {tree_built, Index, IndexN};
                         not_responsible ->
                             {not_responsible, Index, IndexN}
                     end
             end).

as_event(F) ->
    Self = self(),
    spawn(fun() ->
                  Result = F(),
                  gen_fsm:send_event(Self, Result)
          end),
    ok.

%% TODO: where is this timeout handled, or is it just ignored?
do_timeout(S=#state{index=Index, index_n=Preflist}) ->
    lager:info("Timeout during exchange of partition ~p for preflist ~p ",
               [Index, Preflist]),
    maybe_reply({timeout, Index}, S),
    {stop, normal, S}.

maybe_reply(_, S=#state{from=undefined}) ->
    ok,
    S;

maybe_reply(Reply, S=#state{from=Pid,
                                index=Index,
                                index_n=IndexN}) when is_pid(Pid) ->
    %% TODO: make this yz_entropy_mgr:exchange_status
    gen_server:cast(Pid, {exchange_status, self(), Index, IndexN, Reply}),
    S#state{from=undefined};

maybe_reply(Reply, S=#state{from=From}) ->
    %% TODO: who is this reply going to?
    gen_fsm:reply(From, Reply),
    S#state{from=undefined}.

next_state_with_timeout(StateName, State) ->
    next_state_with_timeout(StateName, State, ?DEFAULT_ACTION_TIMEOUT).
next_state_with_timeout(StateName, State, Timeout) ->
    {next_state, StateName, State, Timeout}.
