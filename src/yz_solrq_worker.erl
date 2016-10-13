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

%% solrq/helper interface
-export([batch_complete/2]).

%% drain manager interface
-export([drain/2, drain_complete/1]).

%% TODO: Dynamically pulse_instrument.  See test/pulseh.erl
-ifdef(EQC).
-define(EQC_DEBUG(S, F), ok).
%% -define(EQC_DEBUG(S, F), eqc:format(S, F)).
-else.
-define(EQC_DEBUG(S, F), ok).
-endif.
-define(COUNT_PER_REPORT, 20).

-type solrq_message() :: tuple().  % {BKey, Docs, Reason, P}.
-type pending_processes() :: {pid(), atom()}.
-type drain_info() :: {pid(), reference()} | undefined.
-type worker_state_status() ::
      {all_queue_len, non_neg_integer()}
    | {queue_hwm, non_neg_integer()}
    | {pending_processes, pending_processes()}
    | {drain_info, drain_info()}
    | {purge_strategy, purge_strategy()
    | {queue, yz_queue()}
    | {queue_len, non_neg_integer()}
    | {timer_ref, reference() | undefined}
    | {batch_min, solrq_batch_min()}
    | {batch_max, solrq_batch_max()}
    | {delayms_max, solrq_batch_flush_interval()}
    | {aux_queue, yz_queue()}
    | {draining, boolean() | wait_for_drain_complete}
    | {fuse_blown, boolean()}
    | {in_flight_len, non_neg_integer()}
    | {batch_start, timestamp() | undefined}}.
-type status() :: [worker_state_status()].
-export_type([status/0]).

-record(
    state, {
    index :: index_name(),
    partition :: p(),
    queue_hwm = 1000 :: non_neg_integer(),
    %% Both the vnode and the yz_exchange_fsm can call into `index`.
    %% Therefore, we need a list of processes, not just a single vnode PID
    pending_processes = [] :: [pending_processes()],
    drain_info = undefined :: drain_info(),
    purge_strategy :: purge_strategy(),
    helper_pid = undefined :: pid() | undefined,
    queue = queue:new()     :: yz_queue(),   % solrq_message()
    timer_ref = undefined   :: reference()|undefined,
    batch_min = yz_solrq:get_min_batch_size() :: solrq_batch_min(),
    batch_max = yz_solrq:get_max_batch_size() :: solrq_batch_max(),
    delayms_max = yz_solrq:get_flush_interval() :: solrq_batch_flush_interval(),
    aux_queue = queue:new() :: yz_queue(),
    draining = false        :: boolean() | wait_for_drain_complete,
    fuse_blown = false      :: boolean(),
    in_flight_len = 0       :: non_neg_integer(),
    batch_start             :: timestamp() | undefined
    }).
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
    {ok, State1}.

handle_call({index, E}, From, State0) ->
    ?EQC_DEBUG("index.  State: ~p~n", [debug_state(State0)]),
    State1 = maybe_purge(State0),
    State2 = maybe_send_reply(From, State1),
    State3 = enqueue(E, State2),
    State4 = maybe_send_batch_to_helper(State3),
    FinalState = maybe_start_timer(State4),
    ?EQC_DEBUG("index.  NewState: ~p~n", [debug_state(FinalState)]),
    {noreply, FinalState};
handle_call(status, _From, #state{} = State) ->
    {reply, internal_status(State), State};
handle_call({set_hwm, NewHWM}, _From, #state{queue_hwm = OldHWM} = State) ->
    {reply, {ok, OldHWM}, maybe_unblock_processes(State#state{queue_hwm = NewHWM})};
handle_call(get_hwm, _From, #state{queue_hwm = HWM} = State) ->
    {reply, HWM, State};
handle_call({set_index, Min, Max, DelayMS}, _From, State0) ->
    State1 = State0#state{batch_min = Min,
                          batch_max = Max,
                          delayms_max = DelayMS},
    State2 = maybe_send_batch_to_helper(State1),
    OldParams = {State2#state.batch_min,
                 State2#state.batch_max,
                 State2#state.delayms_max},
    {reply, {ok, OldParams}, State2};
handle_call({set_purge_strategy, NewPurgeStrategy},
    _From,
    #state{purge_strategy=OldPurgeStrategy} = State) ->
    {reply, {ok, OldPurgeStrategy}, State#state{purge_strategy=NewPurgeStrategy}};
handle_call(cancel_drain, _From, State) ->
    NewState = handle_drain_complete(State),
    {reply, ok, NewState};
handle_call(all_queue_len, _From, #state{queue = Queue} = State) ->
    Len = queue:len(Queue),
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
handle_cast({drain, DPid, Token, TargetPartition},
            #state{queue = Queue,
                   in_flight_len = InFlightLen,
                   partition = QueueParitition} = State)
            when TargetPartition == undefined; TargetPartition == QueueParitition ->
    ?EQC_DEBUG("drain{~p=DPid, ~p=Token, ~p=Partition}.  State: ~p~n",
               [DPid, Token, TargetPartition, internal_status(State)]),
    NewState0 = case {queue:is_empty(Queue), InFlightLen} of
        {true, 0} ->
            State#state{draining = wait_for_drain_complete};
        {true, _InFlightLen} ->
            State#state{draining = true};
        _ ->
            drain_queue(State)
        end,
    NewState =
        case NewState0#state.draining of
            wait_for_drain_complete ->
                yz_solrq_drain_fsm:drain_complete(DPid, Token),
                NewState0;
            _ ->
                NewState0#state{drain_info = {DPid, Token}}

        end,
    ?EQC_DEBUG("drain.  NewState: ~p~n", [internal_status(NewState)]),
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
handle_cast(healed_fuse, #state{} = State) ->
    State1 = maybe_start_timer(State#state{fuse_blown = false}),
    State2 = maybe_send_batch_to_helper(State1),
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
%%          thus potentially unblocking any processes waiting on this solrq instance,
%%          if the number of queued messages are above the high water mark.
%% @end
%%
handle_cast({batch_complete, {_NumDelivered, Result}},
        #state{} = State) ->
    ?EQC_DEBUG("batch_complete.  State: ~p~n", [debug_state(State)]),
    State1 = handle_batch(Result, State#state{in_flight_len = 0}),
    State2 = maybe_unblock_processes(State1),
    State3 = maybe_send_batch_to_helper(State2),
    State4 = maybe_start_timer(State3),
    ?EQC_DEBUG("batch_complete.  NewState: ~p~n", [debug_state(State4)]),
    {noreply, State4};

%%
%% @doc     Handle the drain_complete message.
%%
%%          The drain_complete message is sent from the yz_solrq_drain_fsm, once
%%          all the solrqs have completed their drains.  This is a signal to resume
%%          batching, by moving the aux queue to the main queue, setting the drain
%%          flag to false (finally), and kick-starting the indexq.
%% @end
%%
handle_cast(drain_complete, State) ->
    State1 = handle_drain_complete(State),
    {noreply, State1}.

handle_drain_complete(#state{queue = Queue,
                             aux_queue = AuxQueue} = State) ->
    State1 = State#state{
        queue     = queue:join(Queue, AuxQueue),
        aux_queue = queue:new(),
        draining  = false},
    State2 = maybe_send_batch_to_helper(State1),
    State3 = maybe_start_timer(State2),
    State3#state{drain_info = undefined}.


%% @doc Timer has fired - request a helper.
handle_info({timeout, TimerRef, flush},
            #state{timer_ref = TimerRef} = State) ->
    {noreply, flush(State#state{timer_ref = undefined})};

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

%%
%% @doc
%% When we have to retry it does not matter what draining state we are in,
%% we simply have to requeue and continue.
%% end
%%
handle_batch({retry, Undelivered}, State) ->
    State1 = requeue_undelivered(Undelivered, State),
    State1;

%%
%% @doc
%% A drain is not in progress and the result is ok; This means all batched messages were delivered.
%% @end
%%
handle_batch(ok, #state{draining = false, batch_start = T1} = State) ->
    yz_stat:batch_end(?YZ_TIME_ELAPSED(T1)),
    State1 = State#state{batch_start = undefined},
    State1;

%%
%% @doc
%% This queue is being drained and the batch succeeded.
%% If there is nothing left in the queue,
%% wait for the drain_complete message to come back from the
%% drain FSM.
%% @end
%%
handle_batch(ok, #state{draining = true,
                        queue = Queue}=State) ->
    case queue:is_empty(Queue) of
        true ->
            #state{drain_info={DPid, Token}, batch_start = T1} = State,
            yz_stat:batch_end(?YZ_TIME_ELAPSED(T1)),
            yz_solrq_drain_fsm:drain_complete(DPid, Token),
            State#state{draining = wait_for_drain_complete,
                        batch_start = undefined };
        false ->
            State
    end;

%%
%% @doc
%% We are waiting for a drain_complete message to come from the
%% yz_solrq_drain_fsm. Do nothing as we had already emptied
%% our queue.
handle_batch(ok, #state{draining = wait_for_drain_complete} = State) ->
    lager:debug("DBG: Received a batch_complete message while waiting for drain complete"),
    State.

drain_queue(#state{in_flight_len = 0} = State) ->
    %% NB. A drain request may occur while a helper is pending
    %% (and hence while a batch may be "in flight" to Solr).  If
    %% so, then no worker will be requested.  However, the draining
    %% flag will be set on the indexq.  Hence, when the drain_complete
    %% message is received, if there are any pending messages still in
    %% the queue, a worker will be requested at that time, and the indexq
    %% will remain in the draining state, the result being that the
    %% indexq will eventually get drained, once the current in-flight
    %% batch completes.
    State2 = send_batch_to_helper(State),
    State2#state{draining = true};
drain_queue(State) ->
    State#state{draining = true}.

%% @todo: after collapsing the records we might want to reconsider how we do this formatting...
-spec internal_status(state()) -> status().
internal_status(#state{queue = Queue, aux_queue = AuxQueue} = State) ->
    [{F, V} || {F, V} <- ?REC_PAIRS(state, State), F /= queue, F /= aux_queue]
        ++ [{queue_len, queue:len(Queue)}, {aux_queue_len, queue:len(AuxQueue)}].

%% @doc Check HWM, if we are not over it send a reply.
maybe_send_reply(From, #state{pending_processes = PendingProcesses} = State) ->
    case over_hwm(State) of
        true ->
            log_blocked_process(From, State),
            yz_stat:blocked_vnode(From),
            State#state{pending_processes = [From | PendingProcesses] };
        false ->
            gen_server:reply(From, ok),
            State
    end.

log_blocked_process(From, State) ->
    lager:info("Blocking process ~p due to SolrQ ~p exceeding HWM of ~p", [
        From,
        self(),
        State#state.queue_hwm]),
    From.

%% @doc Enqueue the entry and return updated state.
enqueue(E, #state{draining = false,
                  queue = Q} = State) ->
    lager:debug("enqueue to queue, len=~p",
               [queue:len(Q)+1]),
    State#state{queue = queue:in(E, Q)};
enqueue(E, #state{draining = _Draining,
                  aux_queue = A} = State) ->
    lager:debug("enqueue to aux_queue, len=~p",
               [queue:len(A)+1]),
    State#state{aux_queue = queue:in(E, A)}.

%% @doc Re-enqueue undelivered items as part of updated indexq.
requeue_undelivered(Undelivered,
                    #state{queue = Queue} = State) ->
    NewQueue = queue:join(queue:from_list(Undelivered), Queue),
    State#state{queue = NewQueue}.

%% @doc Trigger a flush and return state
flush(#state{in_flight_len = 0} = State) ->
    State2 = send_batch_to_helper(State),
    State2;
flush(State) ->
    State.

%% @doc handle a blown fuse by setting the fuse_blown flag,
%%      purging any data if required, and unblocking any vnodes
%%      if required.
-spec handle_blown_fuse(state()) -> state().
handle_blown_fuse(#state{} = State) ->
    State1 = State#state{fuse_blown = true},
    State2 = maybe_purge(State1),
    State3 = maybe_unblock_processes(State2),
    State3.

%% @doc purge entries depending on purge strategy if we are over the HWM
maybe_purge(#state{fuse_blown = false}=State) ->
    State;
maybe_purge(State) ->
    case queue_has_items(State) andalso over_hwm(State) of
        true ->
            purge(State);
        _ ->
            State
    end.

%% @doc Return true if queue is over the high water mark
%% Note that there is one "in-flight" message so we use >=, not >
over_hwm(#state{queue = Queue, aux_queue = AuxQueue, queue_hwm = HWM, in_flight_len = InFlightLen}) ->
    queue:len(Queue) + queue:len(AuxQueue) + InFlightLen >= HWM.

queue_has_items(#state{queue     = Queue,
                       aux_queue = AuxQueue}) ->
    not (queue:is_empty(Queue) andalso queue:is_empty(AuxQueue)).

%% @todo: should we move this comment? Or is it fine to have it here??
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
-spec purge(state()) -> state().
purge(State) ->
    {NewState, NumPurged} = purge_idx(State),
    case NumPurged of
        0 ->
            ok;
        _ ->
            yz_stat:hwm_purged(NumPurged)
    end,
    NewState.

-spec purge_idx(state()) -> {state(), NumPurged :: non_neg_integer()}.
purge_idx(#state{purge_strategy = ?PURGE_NONE} = State) ->
    {State, 0};
purge_idx(#state{purge_strategy = ?PURGE_ONE,
                 queue=OldQueue,
                 aux_queue=OldAuxQueue,
                 index = Index}=State) ->
    NewState =
        case queue:is_empty(OldAuxQueue) of
            false ->
                NewAuxQueue = maybe_pop_queue(OldAuxQueue),
                State#state{aux_queue=NewAuxQueue};
            true ->
                NewQueue = maybe_pop_queue(OldQueue),
                State#state{queue=NewQueue}
        end,
    ?DEBUG("Removing item from queue because we have hit the high"
           " watermark and the fuse is blown for index ~p, using"
           " purge_strategy ~p", [Index, ?PURGE_ONE]),
    {NewState, 1};
purge_idx(#state{purge_strategy = ?PURGE_IDX,
                 queue = Queue,
                 aux_queue = AuxQueue,
                 index = Index}=State) ->
    QueueLen = queue:len(Queue),
    AuxQueueLen = queue:len(AuxQueue),

    NumPurged = QueueLen + AuxQueueLen,
    NewState = State#state{ queue = queue:new(),
                            aux_queue=queue:new()},
    ?DEBUG("Removing index-queue because we have hit the high"
           " watermark and the fuse is blown for index ~p, using"
           " purge_strategy ~p", [Index, ?PURGE_IDX]),
    %% @todo: do we really need NumPurged now that we don't update the length
    %%        in the state all the time?
    {NewState, NumPurged}.

maybe_pop_queue(Queue) ->
    case queue:out(Queue) of
        {{value, _Item}, NewQueue} -> NewQueue;
        {empty, _Queue} -> Queue
    end.


%% The fuse is blown - just return for now
maybe_send_batch_to_helper(#state{fuse_blown = true} = State) ->
    State;

%% @doc Send a batch to the helper to pull the queue with a provided minimum,
%%      as long as one has not already been requested.
maybe_send_batch_to_helper(#state{in_flight_len = 0,
                                  draining = false,
                                  batch_min = Min,
                                  queue = Queue } = State) ->
    case queue:len(Queue) >= Min of
        true ->
            send_batch_to_helper(State);
        %% We're not above the min, so this is a no-op
        false ->
            State
    end;

%% @doc when we are draining we send a batch even if the batch size is less
%% than the batch_min.
maybe_send_batch_to_helper(#state{draining = true, in_flight_len = 0} = State) ->
    send_batch_to_helper(State);

%% @doc if we didn't take one of the other paths, our in_fight_len is > 0
%% just return State for now
maybe_send_batch_to_helper(State) ->
    State.

%% @doc Push a batch to the helper.
send_batch_to_helper(#state{index = Index,
                            batch_max = BatchMax,
                            helper_pid = HPid} = State) ->
    State1 = maybe_cancel_timer(State),
    {Batch, State2} = prepare_for_batching(State1),
    yz_solrq_helper:index_batch(HPid, Index, BatchMax, self(), Batch),
    case queue:is_empty(State2#state.queue) of
        true ->
            % all the messages have been sent
            State2;
        false ->
            % may be another full batch
            maybe_start_timer(State2)
    end.

%% @doc Get up to batch_max entries.
prepare_for_batching(#state{queue = Q, batch_max = Max} = State) ->
    L = queue:len(Q),
    {BatchQ, RestQ} = queue:split(min(L,Max), Q),
    Batch = queue:to_list(BatchQ),
    BatchLen = length(Batch),
    State1 = State#state{
        batch_start   = os:timestamp(),
        queue         = RestQ,
        in_flight_len = BatchLen
    },
    {Batch, State1}.

%%      If previous `TimerRef' was set, but not yet triggered, cancel it
%%      first as it's invalid for the next batch of this queue.
maybe_cancel_timer(#state{timer_ref=undefined} = State) ->
    State;
maybe_cancel_timer(#state{timer_ref=TimerRef} = State) ->
    erlang:cancel_timer(TimerRef),
    State#state{timer_ref =undefined}.

%% @doc Send replies to blocked processes if under the high water mark
%%      and return updated state
-spec maybe_unblock_processes(state()) -> state().
maybe_unblock_processes(#state{pending_processes = []} = State) ->
    State;
maybe_unblock_processes(#state{pending_processes = PendingProcesses, queue_hwm = QueueHWM} = State) ->
    case over_hwm(State) of
        true ->
            State;
        _ ->
            lists:foreach(fun(PendingProcess) ->
                unblock_process(PendingProcess, QueueHWM) end,
                          PendingProcesses),
            State#state{pending_processes = []}
    end.

unblock_process(PendingProcess, HWM) ->
    lager:debug("Unblocking process ~p due to SolrQ ~p going below HWM of ~p", [
        PendingProcess,
        self(),
        HWM]),
    gen_server:reply(PendingProcess, ok).

maybe_start_timer(#state{delayms_max = infinity}=State) ->
    lager:debug("Infinite delay, will not start timer and flush."),
    State#state{timer_ref = undefined};
maybe_start_timer(#state{timer_ref = undefined,
                         fuse_blown = false,
                         delayms_max = DelayMS,
                         queue = Queue} = State) ->
    case queue:is_empty(Queue) of
        true ->
            State;
        false ->
            TimerRef = erlang:start_timer(DelayMS, self(), flush),
            State#state{timer_ref = TimerRef}
    end;
%% timer already running or blown fuse, so don't start a new timer.
maybe_start_timer(State) ->
    State.

%% @doc Read settings from the application environment
%% TODO: Update HWM for each Index when Ring-Resize occurrs
read_appenv(State) ->
    HWM = app_helper:get_env(?YZ_APP_NAME, ?SOLRQ_HWM, ?SOLRQ_HWM_DEFAULT),
    PBIStrategy = application:get_env(?YZ_APP_NAME, ?SOLRQ_HWM_PURGE_STRATEGY,
                                      ?PURGE_ONE),
    State#state{queue_hwm = HWM,
                purge_strategy = PBIStrategy}.

%%
%% debugging
%%

-compile([{nowarn_unused_function, [
    {debug_queue,1 },
    {debug_indexq, 1},
    {debug_state, 1}]}]).

debug_queue(Queue) ->
    [erlang:element(1, Entry) || Entry <- queue:to_list(Queue)].

debug_indexq(#state{queue = Queue, aux_queue = AuxQueue, draining = Draining,
                    fuse_blown = FuseBlown, in_flight_len = InFlightLen}) ->
    QueueLen = queue:len(Queue),
    [
        {queue, debug_queue(Queue)},
        {queue_len, QueueLen},
        {aux_queue, debug_queue(AuxQueue)},
        {draining, Draining},
        {fuse_blown, FuseBlown},
        {in_flight_len, InFlightLen}
    ].
debug_state(State) ->
    Len = queue:len(State#state.queue),
    [
        {index, State#state.index},
        {all_queue_len, Len},
        {drain_info, State#state.drain_info},
        {batch_start, State#state.batch_start},
        {timer_ref, State#state.timer_ref}
    ] ++ debug_indexq(State).

get_helper(Index, Partition, State0) ->
    HelperName = yz_solrq:helper_regname(Index, Partition),
    HPid = whereis(HelperName),
    State0#state{helper_pid = HPid}.
