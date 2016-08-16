%% -------------------------------------------------------------------
%% Copyright (c) 2015 Basho Technologies, Inc. All Rights Reserved.
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
-module(yz_solrq_worker).

-include("yokozuna.hrl").

-behavior(gen_server).

%% api
-export([start_link/2, status/1, index/5, set_hwm/3, get_hwm/1, set_index/5,
         reload_appenv/2, blown_fuse/1, healed_fuse/1, cancel_drain/1,
         all_queue_len/2, set_purge_strategy/3, stop/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% solrq/helper interface
-export([request_batch/2, drain/2, drain_complete/1, batch_complete/2]).

%% TODO: Dynamically pulse_instrument.  See test/pulseh.erl
-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{gen_server, pulse_gen_server}]}).
-define(PULSE_DEBUG(S,F), pulse:format(S,F)).
-else.
-define(PULSE_DEBUG(S,F), ok).
-endif.
-define(COUNT_PER_REPORT, 20).

-type solrq_message() :: tuple().  % {BKey, Docs, Reason, P}.
-type pending_vnode() :: {pid(), atom()}.
-type drain_info() :: {pid(), reference()} | undefined.
-type indexq_status() ::
      {queue, yz_queue()}
    | {queue_len, non_neg_integer()}
    | {timer_ref, reference() | undefined}
    | {batch_min, solrq_batch_min()}
    | {batch_max, solrq_batch_max()}
    | {delayms_max, solrq_batch_flush_interval()}
    | {aux_queue, yz_queue()}
    | {draining, boolean() | wait_for_drain_complete}
    | {fuse_blown, boolean()}
    | {in_flight_len, non_neg_integer()}
    | {batch_start, timestamp() | undefined}.
-type worker_state_status() ::
      {all_queue_len, non_neg_integer()}
    | {queue_hwm, non_neg_integer()}
    | {pending_vnode, pending_vnode()}
    | {drain_info, drain_info()}
    | {purge_strategy, purge_strategy()}
    | {indexqs, [indexq_status()]}.
-type status() :: [worker_state_status()].
-export_type([status/0]).

-record(
    indexq, {
        queue = queue:new()     :: yz_queue(),   % solrq_message()
        queue_len = 0           :: non_neg_integer(),
        timer_ref = undefined   :: reference()|undefined,
        batch_min = 1           :: solrq_batch_min(),
        batch_max = 100         :: solrq_batch_max(),
        delayms_max = 1000      :: solrq_batch_flush_interval(),
        aux_queue = queue:new() :: yz_queue(),
        draining = false        :: boolean() | wait_for_drain_complete,
        fuse_blown = false      :: boolean(),
        in_flight_len = 0       :: non_neg_integer(),
        batch_start             :: timestamp() | undefined
    }
).

-type indexq() :: #indexq{}.

-record(
    state, {
    index :: index_name(),
    partition :: p(),
    indexq = new_indexq() :: indexq(),
    all_queue_len = 0 :: non_neg_integer(),
    queue_hwm = 1000 :: non_neg_integer(),
    pending_vnode = none :: pending_vnode() | none,
    drain_info = undefined :: drain_info(),
    purge_strategy :: purge_strategy(),
    helper_pid = undefined :: pid() | undefined}
).
-type state() :: #state{}.


%% `record_info' only exists during compilation, so we'll continue to leave
%% this as a macro
-define(REC_PAIRS(Rec,Instance), lists:zip(record_info(fields, Rec),
                                           tl(tuple_to_list(Instance)))).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Index, Partition) ->
    Name = yz_solrq:worker_regname(Index, Partition),
    gen_server:start_link({local, Name}, ?MODULE, [Index, Partition], []).

-spec status(solrq_id()) -> status().
status(QPid) ->
    status(QPid, 5000).

-spec status(solrq_id(), timeout()) -> status().
status(QPid, Timeout) ->
    gen_server:call(QPid, status, Timeout).

-spec index(solrq_id(), bkey(), obj(), write_reason(), p()) -> ok.
index(QPid, BKey, Obj, Reason, P) ->
    gen_server:call(QPid, {index, {BKey, Obj, Reason, P}}, infinity).

-spec set_hwm(Index :: index_name(), Partition :: p(), HWM :: solrq_hwm()) ->
    {ok, OldHWM :: solrq_hwm()} | {error, bad_hwm_value}.
set_hwm(Index, Partition, HWM) when HWM >= 0 ->
    gen_server:call(yz_solrq:worker_regname(Index, Partition), {set_hwm, HWM});
set_hwm(_, _, _)  ->
    {error, bad_hwm_value}.

-spec set_index(index_name(), p(), solrq_batch_min(), solrq_batch_max(),
                solrq_batch_flush_interval()) ->
                    {ok, {solrq_batch_min(),
                         solrq_batch_max(),
                         solrq_batch_flush_interval()}}.
set_index(Index, Partition, Min, Max, DelayMax)
    when Min > 0, Min =< Max, DelayMax >= 0 orelse DelayMax == infinity ->
    gen_server:call(yz_solrq:worker_regname(Index, Partition), {set_index, Min, Max, DelayMax});
set_index(_, _, _, _, _) ->
    {error, bad_index_params}.

-spec set_purge_strategy(index_name(), p(), purge_strategy()) ->
    {ok, OldPurgeStrategy :: purge_strategy()} | {error, bad_purge_strategy}.
set_purge_strategy(Index, Partition, PurgeStrategy)
    when    PurgeStrategy == ?PURGE_NONE
    orelse  PurgeStrategy == ?PURGE_ONE
    orelse  PurgeStrategy == ?PURGE_IDX ->
    gen_server:call(yz_solrq:worker_regname(Index, Partition), {set_purge_strategy, PurgeStrategy});
set_purge_strategy(_Index, _Partition, _PurgeStrategy) ->
    {error, bad_purge_strategy}.

-spec stop(Index::index_name(), Partition::p()) -> any().
stop(Index, Partition) ->
    gen_server:cast(yz_solrq:worker_regname(Index, Partition), stop).

-spec reload_appenv(index_name(), p()) -> ok.
reload_appenv(Index, Partition) ->
    gen_server:call(yz_solrq:worker_regname(Index, Partition), reload_appenv).

-spec drain(solrq_id(), p() | undefined) -> reference().
drain(QPid, Partition) ->
    Token = make_ref(),
    gen_server:cast(QPid, {drain, self(), Token, Partition}),
    Token.

-spec drain_complete(solrq_id()) -> ok.
drain_complete(QPid) ->
    gen_server:cast(QPid, drain_complete).

-spec cancel_drain(solrq_id()) -> ok.
cancel_drain(QPid) ->
    gen_server:call(QPid, cancel_drain).

%% @doc Signal to the solrq that a fuse has blown for the the specified index.
-spec blown_fuse(solrq_id()) -> ok.
blown_fuse(QPid) ->
    gen_server:cast(QPid, blown_fuse).

%% @doc Signal to the solrq that a fuse has healed for the the specified index.
-spec healed_fuse(solrq_id()) -> ok.
healed_fuse(QPid) ->
    gen_server:cast(QPid, healed_fuse).

%% @doc return the sum of the length of all queues in each indexq
-spec get_hwm(solrq_id()) -> HWM :: non_neg_integer().
get_hwm(QPid) ->
    gen_server:call(QPid, get_hwm).

%% @doc return the sum of the length of all queues in each indexq
-spec all_queue_len(Index:: index_name(), Partition::p()) -> non_neg_integer().
all_queue_len(Index, Partition) ->
    gen_server:call(yz_solrq:worker_regname(Index, Partition), all_queue_len).

%%%===================================================================
%%% solrq/helper interface
%%%===================================================================
-spec request_batch(solrq_id(), solrq_helper_id()) -> ok.
request_batch(QPid, HPid) ->
    gen_server:cast(QPid, {request_batch, HPid}).


-spec batch_complete(
    solrq_id(),
    {NumDelivered :: non_neg_integer(), ok} |
    {NumDelivered :: non_neg_integer(),
     {retry, [Undelivered :: solrq_message()]}}) -> ok.
batch_complete(QPid, Message) ->
    gen_server:cast(QPid, {batch_complete, Message}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([Index, Partition]) ->
    State0 = read_appenv(#state{index = Index, partition = Partition}),
    State1 = get_helper(Index, Partition, State0),
    {ok, State1} .

handle_call({index, E}, From, State) ->
    ?PULSE_DEBUG("index.  State: ~p~n", [debug_state(State)]),
    #state{indexq = IndexQ0} = State2 = inc_qlen_and_maybe_unblock_vnode(From, State),
    IndexQ1 = enqueue(E, IndexQ0),
    State3 = State2#state{indexq = IndexQ1},
    #state{indexq = IndexQ2} = State4 = maybe_request_helper(State3),
    IndexQ3 = maybe_start_timer(IndexQ2),
    FinalState = State4#state{indexq = IndexQ3},
    ?PULSE_DEBUG("index.  NewState: ~p~n", [debug_state(FinalState)]),
    {noreply, FinalState};
handle_call(status, _From, #state{} = State) ->
    {reply, internal_status(State), State};
handle_call({set_hwm, NewHWM}, _From, #state{queue_hwm = OldHWM} = State) ->
    {reply, {ok, OldHWM}, maybe_unblock_vnodes(State#state{queue_hwm = NewHWM})};
handle_call(get_hwm, _From, #state{queue_hwm = HWM} = State) ->
    {reply, HWM, State};
handle_call({set_index, Min, Max, DelayMS}, _From, #state{indexq = IndexQ0} = State0) ->
    IndexQ1 = IndexQ0#indexq{batch_min = Min,
        batch_max = Max,
        delayms_max = DelayMS},
    State1 = maybe_request_helper(State0#state{indexq = IndexQ1}),
    OldParams = {IndexQ0#indexq.batch_min,
                 IndexQ0#indexq.batch_max,
                 IndexQ0#indexq.delayms_max},
    {reply, {ok, OldParams}, State1};
handle_call({set_purge_strategy, NewPurgeStrategy},
    _From,
    #state{purge_strategy=OldPurgeStrategy} = State) ->
    {reply, {ok, OldPurgeStrategy}, State#state{purge_strategy=NewPurgeStrategy}};
handle_call(cancel_drain, _From, State) ->
    {noreply, NewState} = handle_cast(drain_complete, State),
    {reply, ok, NewState};
handle_call(all_queue_len, _From, #state{all_queue_len=Len} = State) ->
    {reply, Len, State};
handle_call(reload_appenv, _From, State) ->
    {reply, ok, read_appenv(State)}.

handle_cast(stop, State) ->
    {stop, normal, State};

%%
%% @doc Handle the drain message.
%%
%%      The drain message is sent via the drain/1 function
%%      in this module, which is called by the solrq_drain_fsm
%%      during its prepare state, typically as the result of
%%      a request to drain the queues.
%%
%%      This handler will iterate over all IndexQs in the
%%      solrq, and initiate a drain on the queue, if it is currently
%%      non-empty.
%% @end
%%
%% TODO:
handle_cast({drain, DPid, Token, Partition}, #state{indexq = IndexQ0, partition = Partition} = State) ->
    ?PULSE_DEBUG("drain{~p=DPid, ~p=Token, ~p=Partition}.  State: ~p~n", [debug_state(State), DPid, Token, Partition]),
    NewState0 = case {IndexQ0#indexq.queue_len, IndexQ0#indexq.in_flight_len} of
        {0, 0} ->
            State#state{indexq = IndexQ0#indexq{draining = wait_for_drain_complete}};
        {0, _InFlightLen} ->
            State#state{indexq = IndexQ0#indexq{draining = true}};
        _ ->
            drain_queue(IndexQ0, State)
        end,
    NewState =
        case NewState0#state.indexq#indexq.draining of
            wait_for_drain_complete ->
                yz_solrq_drain_fsm:drain_complete(DPid, Token),
                NewState0;
            _ ->
                NewState0#state{drain_info = {DPid, Token}}

        end,
    ?PULSE_DEBUG("drain.  NewState: ~p~n", [debug_state(NewState)]),
    {noreply, NewState};

%% Totally ignore drain if it's not our partition
handle_cast({drain, DPid, Token, _Partition}, State) ->
    yz_solrq_drain_fsm:drain_complete(DPid, Token),
    {noreply, State};


%% @doc The fuse for the specified index has blown.
handle_cast(blown_fuse, State) ->
    {noreply, handle_blown_fuse(State)};

%%
%% @doc Clear the fuse_blown state on the IndexQ associated with the supplied Index
%%      Resume any batches that may need to proceed.
%% @end
%%
handle_cast(healed_fuse, #state{indexq = IndexQ} = State) ->
    IndexQ2 = maybe_start_timer(IndexQ#indexq{fuse_blown = false}),
    State2 = maybe_request_helper(State#state{indexq = IndexQ2}),
    {noreply, State2};

%%
%% @doc     Handle the batch_complete message.
%%
%%          The batch_complete message is sent via the batch_complete/2 function
%%          in this module, which is called by a solrq_helper when a batch has
%%          been delivered to Solr (or has failed to have been delivered).
%%          If the batch was successfully sent to Solr, the Result field of the
%%          batch_complete message matches {ok, NumDelivered}.  NumDelivered is
%%          the number of messages that were successfully delivered to Solr.
%%          Otherwise, it matches the tuple {retry, NumDelievered, Undelivered},
%%          where NumDelivered is as above, and Undelivered is a list of messages
%%          that have not been delivered (a subset of what was sent for delivery)
%%          and should be retried.  This handler will decrement the all_queues_len
%%          field on the solrq state record by the supplied NumDelievered value
%%          thus potentially unblocking any vnodes waiting on this solrq instance,
%%          if the number of queued messages are above the high water mark.
%% @end
%%
handle_cast({batch_complete, {NumDelivered, Result}},
        #state{all_queue_len = AQL, indexq = IndexQ} = State) ->
    ?PULSE_DEBUG("batch_complete.  State: ~p~n", [debug_state(State)]),
    State1 = handle_batch(IndexQ#indexq{in_flight_len = 0}, Result, State),
    NewQueueLength = AQL - NumDelivered,
    NewState0 = maybe_unblock_vnodes(State1#state{all_queue_len = NewQueueLength}),
    NewState = maybe_request_helper(NewState0),
    ?PULSE_DEBUG("batch_complete.  NewState: ~p~n", [debug_state(NewState)]),
    {noreply, NewState};

%%
%% @doc     Handle the drain_complete message.
%%
%%          The drain_complete message is sent from the yz_solrq_drain_fsm, once
%%          all the solrqs have completed their drains.  This is a signal to resume
%%          batching, by moving the aux queue to the main queue, setting the drain
%%          flag to false (finally), and kick-starting the indexq.
%% @end
%%
handle_cast(drain_complete, #state{indexq = IndexQ0, index = Index, partition = Partition} = State) ->
    lager:debug("Solrq drain worker received drain_complete messages for index/partition ~p/~p", [Index, Partition]),
    #indexq{queue = Queue, queue_len = QueueLen, aux_queue = AuxQueue} = IndexQ0,
    IndexQ1 = IndexQ0#indexq{
        queue = queue:join(Queue, AuxQueue),
        queue_len = QueueLen + queue:len(AuxQueue),
        aux_queue = queue:new(),
        draining = false
    },
    State1 = maybe_request_helper(State#state{indexq = IndexQ1}),
    IndexQ2 = maybe_start_timer(State1#state.indexq),

    {noreply, State1#state{drain_info = undefined, indexq = IndexQ2}}.


%% @doc Timer has fired - request a helper.
handle_info({timeout, TimerRef, flush}, #state{indexq = IndexQ} = State)
    when TimerRef == IndexQ#indexq.timer_ref ->
    case timer_running(IndexQ) of
        true ->
            {noreply,
             flush(IndexQ#indexq{timer_ref = undefined},
                   State)};
        false -> % out of date
            {noreply, State}
    end;

handle_info({timeout, _TimerRef, flush}, State) ->
    lager:debug("Received timeout from stale Timer Reference"),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Check if timer (erlang:start_timer/3) ref is defined.
-spec timer_running(indexq()) -> boolean().
timer_running(#indexq{timer_ref = undefined}) -> false;
timer_running(_) -> true.

%%
%% @doc
%% A drain is not in progress.  There are two cases to consider:
%%   1. The result is ok; This means all batched messages were delivered.
%%   2. The solrq_helper returned some undelivered messages; pre-pend these to the queue
%%      for this index, and request a new helper, if we are over the requested minimum.
%% @end
%%
handle_batch(
    #indexq{draining = false, batch_start = T1} = IndexQ0,
    Result,
    State
) ->
    IndexQ1 =
        case Result of
            ok ->
                yz_stat:batch_end(?YZ_TIME_ELAPSED(T1)),
                IndexQ0#indexq{batch_start = undefined};
            {retry, Undelivered} ->
                requeue_undelivered(Undelivered, IndexQ0)
        end,
    #state{indexq = IndexQ2} = State1 = maybe_request_helper(State#state{indexq = IndexQ1}),
    IndexQ3 = maybe_start_timer(IndexQ2),
    State1#state{indexq = IndexQ3};

%%
%% @doc
%% This queue is being drained.  There are two cases to consider:
%%    1. The batch succeeded.  If there is nothing left in the queue,
%%       then remove ourselves from the list
%%       of remaining indices that need to be flushed, and
%%       wait for t a drain_complete message to come back from the
%%       drain FSM.
%%    2. The batch did not succeed.  In this case, we got back a list
%%       of undelivered messages.  Put the undelivered messages back onto
%%       the queue and request another helper.
%%
%% If there are no remaining indexqs to drain, then send the drain FSM
%% a drain_complete message for this solrq.
%% @end
%%
handle_batch(
  #indexq{queue_len = QueueLen, draining = _Draining, batch_start = T1} = IndexQ,
  Result,
  #state{drain_info = DrainInfo} = State) ->
    {DPid, Token} = DrainInfo,
    NewState =
        case Result of
            ok ->
                case QueueLen of
                    0 ->
                        yz_stat:batch_end(?YZ_TIME_ELAPSED(T1)),
                        IndexQ2 = IndexQ#indexq{
                                draining = wait_for_drain_complete,
                                batch_start = undefined },
                        State#state{indexq = IndexQ2};
                    _ ->
                        send_batch_to_helper(IndexQ, State)
                end;
            {retry, Undelivered} ->
                State2 = send_batch_to_helper(IndexQ, State),
                IndexQ2 = requeue_undelivered(Undelivered, State2#state.indexq),
                State2#state{indexq = IndexQ2}
        end,
    %%
    %% If there are no remaining indexqs to be flushed, send the drain FSM
    %% a drain complete for this solrq instance.
    %%
    NewIndexQ = NewState#state.indexq,
    case NewIndexQ#indexq.draining of
        wait_for_drain_complete ->
            yz_solrq_drain_fsm:drain_complete(DPid, Token),
            NewState;
        _ ->
            NewIndexQ2 = maybe_start_timer(NewIndexQ),
            State#state{drain_info = {DPid, Token}, indexq=NewIndexQ2}
    end.

drain_queue(IndexQ, State) ->
    %% NB. A drain request may occur while a helper is pending
    %% (and hence while a batch may be "in flight" to Solr).  If
    %% so, then no worker will be requested.  However, the draining
    %% flag will be set on the indexq.  Hence, when the drain_complete
    %% message is received, if there are any pending messages still in
    %% the queue, a worker will be requested at that time, and the indexq
    %% will remain in the draining state, the result being that the
    %% indexq will eventually get drained, once the current in-flight
    %% batch completes.
    State2 = send_batch_to_helper(IndexQ, State),
    State2#state.indexq#indexq{draining = true}.

-spec internal_status(state()) -> status().
internal_status(#state{indexq = IndexQ} = State) ->
    [{F, V} || {F, V} <- ?REC_PAIRS(state, State), F /= indexq] ++
    [{indexq, [{F, V} || {F, V} <- ?REC_PAIRS(indexq, IndexQ), F /= queue]}].

%% @doc Increment the aggregated queue length and see if the vnode needs to be
%%      stalled.
inc_qlen_and_maybe_unblock_vnode(From, #state{all_queue_len = AQL}
                                 = State) ->
    State2 = State#state{all_queue_len = AQL + 1},
    case over_hwm(State2) of
        true ->
            State3 = maybe_purge_blown_indices(State2),
            case over_hwm(State3) of
                true ->
                    log_blocked_vnode(From, State),
                    yz_stat:blocked_vnode(From),
                    State3#state{pending_vnode = From};
                false ->
                    gen_server:reply(From, ok),
                    State3
            end;
        false ->
            gen_server:reply(From, ok),
            State2
    end.

log_blocked_vnode(From, State) ->
    lager:debug("Blocking vnode ~p due to SolrQ ~p exceeding HWM of ~p", [
        From,
        self(),
        State#state.queue_hwm]),
    From.

%% @doc Enqueue the entry and return updated state.
enqueue(E, #indexq{queue = Q, queue_len = L, aux_queue = A, draining = Draining} = IndexQ) ->
    case Draining of
        false ->
            IndexQ#indexq{queue = queue:in(E, Q), queue_len = L + 1};
        _ ->
            IndexQ#indexq{aux_queue = queue:in(E, A)}
    end.

%% @doc Re-enqueue undelivered items as part of updated indexq.
requeue_undelivered(Undelivered, #indexq{queue = Queue, queue_len = QueueLen} = IndexQ) ->
    NewQueue = queue:join(queue:from_list(Undelivered), Queue),
    IndexQ#indexq{
      queue = NewQueue,
      queue_len = QueueLen + erlang:length(Undelivered)
     }.

%% @doc Trigger a flush and return state
flush(IndexQ, State) ->
    send_batch_to_helper(IndexQ, State).

%% @doc handle a blown fuse by setting the fuse_blown flag,
%%      purging any data if required, and unblocking any vnodes
%%      if required.
-spec handle_blown_fuse(state()) -> state().
handle_blown_fuse(#state{purge_strategy=PBIStrategy, indexq = IndexQ} = State) ->
    IndexQ2 = IndexQ#indexq{fuse_blown = true},
    State1 = State#state{indexq = IndexQ2},
    State2 = maybe_purge(IndexQ, PBIStrategy, State1),
    maybe_unblock_vnodes(State2);
handle_blown_fuse(State) ->
    State.

%% @doc purge entries depending on purge strategy if we are over the HWM
maybe_purge(Index, PBIStrategy, State) ->
    case over_hwm(State) of
        true ->
            purge(Index, PBIStrategy, State);
        _ ->
            State
    end.

%% @doc Return true if queue is over the high water mark
over_hwm(#state{all_queue_len = L, queue_hwm = HWM}) ->
    L > HWM.

maybe_purge_blown_indices(#state{purge_strategy=?PURGE_NONE} = State) ->
    State;
maybe_purge_blown_indices(#state{indexq=IndexQ,
                                 purge_strategy=PBIStrategy}
                          = State) ->
    #indexq{fuse_blown=IsBlown,
            queue_len=QueueLen,
            aux_queue=AuxQueue} = IndexQ,
    HasItems = (QueueLen > 0) orelse (queue:len(AuxQueue) > 0),
    case IsBlown andalso HasItems of
        false ->
            State;
        _ ->
            purge(IndexQ, PBIStrategy, State)
    end.

%% @doc If we hit the high water mark, we will attempt to purge previously
%% enqueued entries based on a purge strategy, which may be one of:
%%
%% purge_one:   Purge exactly one entry from a randomly blown indexq.  Prefer
%%              to purge from the aux_queue, if it is not empty.
%% purge_index: Purge all entries (both in the regular queue and in the
%%              aux_queue) from a randomly blown indexq
%% purge_all:   Purge all entries (both in the regular queue and in the
%%              aux_queue) from all blown indexqs
%%
-spec purge(indexq(), purge_strategy(), state()) ->
    state().
purge(IndexQ, PurgeStrategy, State) ->
    {NewState, NumPurged} = purge_idx(IndexQ, PurgeStrategy, State),
    case NumPurged of
        0 ->
            ok;
        _ ->
            yz_stat:hwm_purged(NumPurged)
    end,
    NewState.

-spec purge_idx(BlownIndex :: indexq(),
                purge_strategy(),
                state()) -> {state(), NumPurged :: non_neg_integer()}.
purge_idx(_, ?PURGE_NONE, State) ->
    {State, 0};
purge_idx(
   #indexq{queue=OldQueue, aux_queue=OldAuxQueue}=IndexQ,
  ?PURGE_ONE,
  #state{all_queue_len=AllQueueLen, index = Index} = State) ->
    OldAuxQueueLen = queue:len(OldAuxQueue),
    NewIndexQ =
        case OldAuxQueueLen > 0 of
            true ->
                NewAuxQueue = maybe_pop_queue(OldAuxQueue),
                IndexQ#indexq{aux_queue=NewAuxQueue};
            _ ->
                NewQueue = maybe_pop_queue(OldQueue),
                IndexQ#indexq{queue=NewQueue, queue_len=queue:len(NewQueue)}
        end,
    ?DEBUG("Removing item from queue because we have hit the high"
           " watermark and the fuse is blown for index ~p, using"
           " purge_strategy ~p", [Index, ?PURGE_ONE]),
    {State#state{all_queue_len= AllQueueLen - 1, indexq = NewIndexQ}, 1};
purge_idx(#indexq{queue_len=QueueLen, aux_queue=AuxQueue}=IndexQ,
          ?PURGE_IDX,
          #state{all_queue_len=AllQueueLen, index=Index} = State) ->
    AuxQueueLen = queue:len(AuxQueue),
    NumPurged = QueueLen + AuxQueueLen,
    NewIndexQ = IndexQ#indexq{queue=queue:new(), aux_queue=queue:new(),
                              queue_len=0},
    ?DEBUG("Removing index-queue because we have hit the high"
           " watermark and the fuse is blown for index ~p, using"
           " purge_strategy ~p", [Index, ?PURGE_IDX]),
    {State#state{all_queue_len = AllQueueLen - NumPurged, indexq = NewIndexQ}, NumPurged}.

maybe_pop_queue(Queue) ->
    case queue:out(Queue) of
        {{value, _Item}, NewQueue} -> NewQueue;
        {empty, _Queue} -> Queue
    end.

%% @doc Request a helper to pull the queue with a provided minimum,
%%      as long as one has not already been requested.
maybe_request_helper(#state{indexq = IndexQ = #indexq{fuse_blown = false, batch_min = Min, queue_len = L}} = State)
    when L >= Min ->
    send_batch_to_helper(IndexQ, State);
%% Already have a pending helper, the fuse is blown, or we're not above the min,
%% so this is a no-op
maybe_request_helper(State) ->
    State.

%% @doc Notify a solrq helper the index is ready to be pulled.
send_batch_to_helper(#indexq{in_flight_len = InFlightLen},
                     #state{helper_pid = HPid} = State) when InFlightLen == 0 ->
    send_entries(HPid, State);
send_batch_to_helper(IndexQ, State) ->
    State#state{indexq = IndexQ}.

%% @doc Send a batch of entries, reply to any blocked vnodes and
%%      return updated state
send_entries(HPid, #state{indexq = IndexQ, index = Index} = State) ->
    #indexq{batch_max = BatchMax} = IndexQ,
    {Batch, _BatchLen, IndexQ2} = get_batch(IndexQ),
    yz_solrq_helper:index_batch(HPid, Index, BatchMax, self(), Batch),
    case IndexQ2#indexq.queue_len of
        0 ->
            % all the messages have been sent
            State#state{indexq = IndexQ2};
        _ ->
            % may be another full batch
            IndexQ3 = maybe_start_timer(IndexQ2),
            State#state{indexq = IndexQ3}
    end.

%% @doc Get up to batch_max entries and reset the pending worker/timer ref.
get_batch(#indexq{queue = Q, queue_len = L, batch_max = Max,
                  draining = Draining} = IndexQ0) ->
    IndexQ1 = maybe_cancel_timer(IndexQ0),
    {BatchQ, RestQ} =
        case Draining of
            true ->
                {Q, queue:new()};
            _ ->
                queue:split(min(L,Max), Q)
        end,
    Batch = queue:to_list(BatchQ),
    BatchLen = length(Batch),
    IndexQ2 = IndexQ1#indexq{queue = RestQ, queue_len = L - BatchLen,
                            in_flight_len = BatchLen,
                            batch_start = os:timestamp()},
    {Batch, BatchLen, IndexQ2}.

%%      If previous `TimerRef' was set, but not yet triggered, cancel it
%%      first as it's invalid for the next batch of this IndexQ.
maybe_cancel_timer(#indexq{timer_ref=undefined} = IndexQ) ->
	IndexQ;
maybe_cancel_timer(#indexq{timer_ref=TimerRef} = IndexQ) ->
    erlang:cancel_timer(TimerRef),
    IndexQ#indexq{timer_ref=undefined}.

%% @doc Send replies to blocked vnodes if under the high water mark
%%      and return updated state
-spec maybe_unblock_vnodes(state()) -> state().
maybe_unblock_vnodes(#state{pending_vnode = none} = State) ->
    State;
maybe_unblock_vnodes(#state{pending_vnode = PendingVnode} = State) ->
    case over_hwm(State) of
        true ->
            State;
        _ ->
            lager:debug("Unblocking vnode ~p due to SolrQ ~p going below HWM of ~p", [
                PendingVnode,
                self(),
                State#state.queue_hwm]),
            gen_server:reply(PendingVnode, ok),
            State#state{pending_vnode = none}
    end.

maybe_start_timer(#indexq{timer_ref = undefined, queue_len = L,
                                 fuse_blown = false,
                                 delayms_max = DelayMS} = IndexQ) when L > 0 ->
    TimerRef =
        case DelayMS of
            infinity ->
                lager:debug("Infinite delay, will not start timer and flush."),
                undefined;
            _ ->
                %% TODO: remove Index - we only have one
                erlang:start_timer(DelayMS, self(), flush)
        end,
    IndexQ#indexq{timer_ref = TimerRef};
maybe_start_timer(IndexQ) ->
    IndexQ.

set_new_index(Min, Max, DelayMS)
  when Min > 0, Min =< Max, DelayMS >= 0 orelse DelayMS == infinity ->
    #indexq{batch_min = Min,
            batch_max = Max,
            delayms_max = DelayMS};
set_new_index(_, _, _) ->
    #indexq{}.

new_indexq() ->
    BatchMin = yz_solrq:get_min_batch_size(),
    BatchMax = yz_solrq:get_max_batch_size(),
    DelayMS = yz_solrq:get_flush_interval(),
    set_new_index(BatchMin, BatchMax, DelayMS).

%% @doc Read settings from the application environment
%% TODO: Update HWM for each Index when Ring-Resize occurrs
read_appenv(State) ->
    HWM = app_helper:get_env(?YZ_APP_NAME, ?SOLRQ_HWM, 1),
    PBIStrategy = application:get_env(?YZ_APP_NAME, ?SOLRQ_HWM_PURGE_STRATEGY,
                                      ?PURGE_ONE),
    State#state{queue_hwm = HWM,
                purge_strategy = PBIStrategy}.

%%
%% debugging
%%

-ifdef(PULSE).
debug_queue(Queue) ->
    [erlang:element(1, Entry) || Entry <- queue:to_list(Queue)].

debug_indexq(#indexq{queue = Queue, queue_len = QueueLen, aux_queue = AuxQueue, draining = Draining, fuse_blown = FuseBlown, in_flight_len = InFlightLen}) ->
    [
        {queue, debug_queue(Queue)},
        {queue_len, QueueLen},
        {aux_queue, debug_queue(AuxQueue)},
        {draining, Draining},
        {fuse_blown, FuseBlown},
        {in_flight_len, InFlightLen}
    ].
debug_state(State) ->
    [
        {indexq, State#state.indexq},
        {index, State#state.index},
        {all_queue_len, State#state.all_queue_len},
        {drain_info, State#state.drain_info}
    ].
-endif.

get_helper(Index, Partition, State0) ->
    HelperName = yz_solrq:helper_regname(Index, Partition),
    HPid = whereis(HelperName),
    State0#state{helper_pid = HPid}.