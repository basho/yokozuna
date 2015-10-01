%% Smaller props for just solrq
%%  index should be blocked when all index send >= hwm
%   batches should be between min/max size
%   ready should be called within the delayms_max
%   ready should only be called once before servicing
-module(yz_solrq_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_component.hrl").
-compile({parse_transform, eqc_cover}).

-compile(export_all).

-record(tstate, {hwm = 3,
                 batch_min = 2,
                 batch_max = 2,
                 started = false,
                 index_call = 1,
                 blocked = [],
                 inserted_by_index = orddict:new(),
                 batched_by_index = orddict:new(),
                 timers = [],
                 ready = [],
                 pending_by_index = orddict:new()}).

run() ->
    run(20).

run(Secs) ->
    eqc:quickcheck(eqc:testing_time(Secs, prop_solrq())).

features(Secs) ->
    eqc_suite:feature_based(eqc:testing_time(Secs, prop_solrq())).

check() ->
    eqc:check(eqc_statem:show_states(prop_solrq())).


cover(Secs) ->
    eqc_cover:start(),
    eqc:quickcheck(eqc:testing_time(Secs, prop_solrq())),
    Data = eqc_cover:stop(),
    eqc_cover:write_html(Data, []).


gen_index() ->
    elements(["index1","index2"]). % TODO: Convert back to binaries

gen_bt() ->
    elements([<<"bt1">>,<<"bt2">>, <<"bt_no_index">>]).

gen_bucket() ->
    elements([<<"bn1">>, <<"bn2">>, <<"bn_no_index">>]).

gen_key() ->
    elements([<<"k1">>,<<"k2">>,<<"k3">>,<<"k4">>,<<"k5">>]).

gen_val() ->
    noshrink(non_empty(binary())).

gen_obj() ->
    ?LET({BT,BN,K,V},{gen_bt(), gen_bucket(), gen_key(), gen_val()},
        riak_object:new({BT,BN},K,V,"text/plain")).



gen_reason() ->
    oneof([put, delete, handoff]).

gen_partition() -> % generate 2^160 space partitions
    0.

initial_state() ->
    #tstate{}.

%% ------ Common pre-/post-conditions
command_precondition_common(#tstate{started = Started}, start) ->
    not Started;
command_precondition_common(#tstate{started = Started}, _Cmd) ->
    Started.


precondition_common(_S, _Call) ->
    true.

postcondition_common(_S, _Call, _Res) ->
    true.

%% --- Operation: start ---
start_args(_S) ->
    ?LET({HWMSeed, MinSeed, MaxSeed},
         {nat(), nat(), nat()},
         [1 + HWMSeed, 1 + MinSeed, 1 + MinSeed + MaxSeed]).

start(HWM, Min, Max) ->
    %% For now, set batch min, batch max, queue_hwm to 1
    %% TODO: Generate index parameters - not sure what to do per-index.
    application:set_env(yokozuna, solrq_queue_hwm, HWM),
    application:set_env(yokozuna, solrq_batch_min, Min),
    application:set_env(yokozuna, solrq_batch_max, Max),
    application:set_env(yokozuna, solrq_delayms_max, 1000),

    %% And restart with a nice clean process
    restart(yz_solrq_sup, yz_solrq_0001),

    ok.

start_next(S, _Value, [HWM, Min, Max]) ->
    S#tstate{hwm = HWM,
             batch_min = Min,
             batch_max = Max,
             started = true}.


%% ------ Grouped operator: op
index_args(#tstate{index_call = Call}) -> [Call, gen_index(), gen_obj(), gen_reason(), gen_partition()].

index(_Call, Index, Obj, Reason, Partition) ->
    BKey = bkey(Obj),
    delay(yz_solrq:index(Index, BKey, Obj, Reason, Partition)).


index_callouts(#tstate{hwm = HWM, batch_min = BatchMin} = S,
               [Call, Index, _Obj, _Reason, _Partition]) ->
    WillBlock = (queued_total(S) >= HWM),
    ?PAR(?WHEN(WillBlock, %% If blocks, expect stats update and block
               ?CALLOUT(exometer, update, [[riak, yokozuna, index, blockedvnode], 1], ok)),
         ?WHEN(queued_index(Index, S) == BatchMin - 1, % If will hit min size, request helper
               index_ready_callout(Index))),
    ?WHEN(queued_index(Index, S) == 0 andalso BatchMin > 1, % if first entry and not large enough to batch
          send_after_callout()),
    ?WHEN(WillBlock, %% If blocks, expect stats update and block
         ?SEQ([ ?APPLY(add_blocked, [?SELF]),
                ?BLOCK,
                ?APPLY(del_blocked, [?SELF])])).

index_ready_callout(Index) ->
    ?CALLOUTS(
       ?MATCH({QPid, _Result},
              ?CALLOUT(yz_solrq_helper, index_ready, [?WILDCARD, Index, ?VAR], ok)),
       ?APPLY(index_ready, [Index, QPid])).

send_after_callout() ->
    ?CALLOUTS(
       ?MATCH({Pid, Msg, _Res},
              ?CALLOUT(yz_solrq_timer, send_after, [?WILDCARD, ?VAR, ?VAR], ok)),
       ?APPLY(send_after, [Pid, Msg])).


index_next(#tstate{pending_by_index = Pending, inserted_by_index = Inserted} = S, _Value,
           [Index, {K, _Obj}, Reason, Partition]) ->
    S#tstate{index_call = S#tstate.index_call + 1,
                            pending_by_index = orddict:update_counter(Index, 1, Pending),
             inserted_by_index = orddict:append_list(Index, [{K, Reason, Partition}], Inserted)}.

index_post(_S, _Args, _Res) ->
    true.


%% Triggered by callout
index_ready_next(#tstate{ready = Ready} = S, _Value, [Index, QPid]) ->
    S#tstate{ready = [{Index, QPid} | Ready]}.


%% --- Operation: request_batch --- request batch from helper

request_batch_pre(S) ->
    Test = S#tstate.ready /= [],
    %io:format("request_batch_pre(~p) -> ~p\n", [S, Test]),
    Test.

request_batch_args(S) ->
    [elements(S#tstate.ready)].

request_batch_pre(S, [{_Index, _QPid} = Req]=_A) -> % for shrinking
    Test = lists:member(Req, S#tstate.ready),
    %io:format("request_batch_pre(~p,~p) -> ~p\n", [S, _A, Test]),
    Test.

request_batch_callouts(#tstate{hwm = HWM, blocked = Blocked, batch_min = Min} = S,
                       [{Index, _QPid}]) ->
    Remainder = queued_index(Index, S) - expected_batch_size(Index, S),
    ?MATCH({Docs, _Res},
           ?CALLOUT(yz_solrq_helper, index_batch, [?WILDCARD, Index, ?VAR], ok)),
    ?APPLY(index_batch, [Index, Docs]),
    %% ?MATCH({Docs, _Result}, %TODO: Add check for min/max lengths
    %%        ?CALLOUT(yz_solrq_helper, index_batch, [?WILDCARD, Index, ?VAR], ok)),
    ?WHEN(Remainder >= Min,
          index_ready_callout(Index)),
    ?WHEN(0 < Remainder andalso Remainder < Min,
          send_after_callout()),
    ?WHEN((queued_total(S) - expected_batch_size(Index, S)) =< HWM, %% Unblock if was blocked and cleared
          ?PAR([?UNBLOCK(SolrqCall, ok) || SolrqCall <- Blocked])). % could be empty


request_batch({Index, QPid}) ->
    delay(yz_solrq:request_batch(QPid, Index, self())).

request_batch_next(#tstate{ready = Ready, pending_by_index = Pending} = S, _Value, [{Index, _QPid}] = Requested) ->
    ExpectedSize = expected_batch_size(Index, S),
    S1 = S#tstate{ready = Ready -- Requested,
                  pending_by_index = orddict:update_counter(Index, -ExpectedSize, Pending)},
    case over_hwm(S1) of
        false -> % if under HWM, everything should be unblocked
            S1#tstate{blocked = []};
        _ ->
            S1 % otherwise, stay blocked
    end.


%% --- Operation: index_batch --- send an index batch to an awaiting helper

index_batch_next(#tstate{batched_by_index = Batched} = S, _Value, [Index, Docs]) ->
    % wrap Docs in a list as it it will be symbolic
    S#tstate{batched_by_index = orddict:append_list(Index, [Docs], Batched)}.

%% --- Operation: send_after --- timer fired

send_after_next(#tstate{timers = Timers} = S, _Value, [Pid, Msg]) ->
    S#tstate{timers = [{Pid, Msg} | Timers]}.

fire_timer_pre(S) ->
    Test = S#tstate.timers /= [],
    %io:format("fire_timer_pre(~p) -> ~p\n", [S, Test]),
    Test.

fire_timer_args(S) ->
    [elements(S#tstate.timers)].

fire_timer_pre(S, [{_Index, _QPid} = Req]=_A) -> % for shrinking
    Test = lists:member(Req, S#tstate.ready),
    %io:format("fire_timer_pre(~p,~p) -> ~p\n", [S, _A, Test]),
    Test.

fire_timer_callouts(_S, _Timer) ->
    %% TODO: Work it out.  Problem is you have to track whether the timer has already been handled or not.
    ?EMPTY.

fire_timer({Pid, Msg}) ->
    Pid ! Msg.


fire_timer_next(#tstate{timers = Timers} = S, _Value, [{_Pid, _Msg}] = Requested) ->
    S#tstate{timers = Timers -- Requested}.


%% ------ ... more operations

weight(_S, _Cmd) -> 1.

prop_solrq() ->
    with_parameter(print_counterexample, false,
    with_parameter(default_process, worker,
    ?SETUP(fun() -> setup(), fun() -> teardown() end end,
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {H, S, Res} = run_commands(?MODULE,Cmds),
                features([{work_pending, {S#tstate.index_call, work_pending(S)}}],
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                aggregate(command_names(Cmds),
                                conjunction([{result, equals(Res, ok)},
                                             {sent, (work_pending(S) orelse
                                                     equals(inserted_by_index(S),
                                                            batched_by_index(S)))}]))))
            end)))).


%% Helpers


%% Return true if there is any work left in the system (timers to fire or 
%% requests to service)
work_pending(#tstate{ready = Ready, timers = Timers}) ->
    Ready /= [] orelse Timers /= [].


%% Return the bkey for an object
bkey(Obj) ->
    B = riak_object:bucket(Obj),
    K = riak_object:key(Obj),
    {B, K}.

%% Rebuild the bkeys to match the insert call {index, Index, {BKey, Obj, Reason, P}}, infinity)
inserted_by_index(#tstate{inserted_by_index = Inserted}) ->
    orddict:map(fun(_Index, Entries) ->
                        [ {bkey(Obj), Obj, Reason, Partition} || {Obj, Reason, Partition} <- Entries]
                end, Inserted).

%% The batches of documents are lists of lists as the model needs to tolerate
%% symbolic variables as documents
batched_by_index(#tstate{batched_by_index = Batched}) ->
    orddict:map(fun(_Index, Delivered) -> lists:flatten(Delivered) end, Batched).


delay(Res) ->
    % timer:sleep(5),
    Res.

over_hwm(#tstate{hwm = HWM} = S) ->
    queued_total(S) > HWM.

add_blocked_next(#tstate{blocked = Blocked} = S, _, [Pid]) ->
    S#tstate{blocked = Blocked ++ [Pid]}.

del_blocked_next(#tstate{blocked = Blocked} = S, _, [Pid]) ->
    S#tstate{blocked = Blocked -- [Pid]}.

queued_index(Index, #tstate{pending_by_index = Pending}) ->
    case orddict:find(Index, Pending) of
        {ok, Count} ->
            Count;
        error ->
            0
    end.

queued_total(#tstate{pending_by_index = Pending}) ->
    orddict:fold(fun(_Index, Count, Acc) ->
                         Acc + Count
                 end, 0, Pending).

expected_batch_size(Index, #tstate{batch_max = Max} = S) ->
    min(queued_index(Index, S), Max).

restart(Sup, Id) ->
    catch supervisor:terminate_child(Sup, Id),
    {ok, _} = supervisor:restart_child(Sup, Id).

unlink_kill(Name) ->
    try
        Pid = whereis(Name),
        unlink(Pid),
        exit(Pid, kill)
    catch _:_ ->
            true
    end.

%% Setup things...
setup() ->
    {ok, Pid} = yz_solrq_sup:start_link(1, 1),
    unlink(Pid),  %% We observe a crash of the supervisor differently
    eqc_mocking:start_mocking(api_spec()).

teardown() ->
    unlink_kill(yz_solrq_sup),
    eqc_mocking:stop_mocking().

prepare() ->
    ok.

api_spec() ->
    #api_spec{ modules = [yz_solrq_timer(),
                          yz_solrq_helper(),
                          exometer()] }.

yz_solrq_timer() ->
    #api_module{ name = yz_solrq_timer, fallback = ?MODULE,
                 functions = [#api_fun{ name = send_after, arity = 3, fallback = true }]}.

yz_solrq_helper() ->
    #api_module{ name = yz_solrq_helper, fallback = undefined,
                 functions = [#api_fun{ name = index_ready, arity = 3},
                              #api_fun{ name = index_batch, arity = 3}]}.

exometer() ->
    #api_module{ name = exometer,
                 functions = [ #api_fun{ name = update, arity = 2 } ]}.


send_after(Delay, Pid, Msg) ->
    io:format("Setup timer in ~p to ~p message ~p\n",[Delay, Pid, Msg]).

%% %% THE MOCKED FALLBACK
%% %% If bt_no_index or bn_no_index
%% get_bucket({<<"bt_no_index">>,BN}) ->
%%     [{n_val, 3}];
%% get_bucket({BT,<<"bn_no_index">>}) ->
%%     [{n_val, 3}];
%% get_bucket({BT,BN}=B) ->
%%     [{search_index, <<"index1">>}, {n_val, 3}].

%% Also
