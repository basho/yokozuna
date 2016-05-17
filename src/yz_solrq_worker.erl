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
-export([start_link/1, status/1, index/6, set_hwm/2, get_hwm/1, set_index/5,
         reload_appenv/1, blown_fuse/2, healed_fuse/2, cancel_drain/1,
         all_queue_len/1, set_purge_strategy/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% solrq/helper interface
-export([request_batch/3, drain/2, drain_complete/1, batch_complete/3]).

%% TODO: Dynamically pulse_instrument.  See test/pulseh.erl
-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{gen_server, pulse_gen_server}]}).
-define(PULSE_DEBUG(S,F), pulse:format(S,F)).
-else.
-define(PULSE_DEBUG(S,F), ok).
-endif.

-type solrq_message() :: tuple().  % {BKey, Docs, Reason, P}.
-type pending_vnodes() :: [{pid(), atom()}].
-type drain_info() :: {pid(), reference(), [solrq_message()]} | undefined.
-type indexq_status() ::
      {queue, yz_queue()}
    | {queue_len, non_neg_integer()}
    | {timer_ref, reference() | undefined}
    | {pending_helper, boolean()}
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
    | {pending_vnodes, pending_vnodes()}
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
        pending_helper = false  :: boolean(),
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

-record(
    state, {
      indexqs = dict:new()             :: yz_dict(),
      all_queue_len = 0                :: non_neg_integer(),
      queue_hwm = 1000                 :: non_neg_integer(),
      pending_vnodes = []              :: pending_vnodes(),
      drain_info = undefined           :: drain_info(),
      purge_strategy                   :: purge_strategy()
    }
).


%% `record_info' only exists during compilation, so we'll continue to leave
%% this as a macro
-define(REC_PAIRS(Rec,Instance), lists:zip(record_info(fields, Rec),
                                           tl(tuple_to_list(Instance)))).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

-spec status(solrq_id()) -> status().
status(QPid) ->
    status(QPid, 5000).

-spec status(solrq_id(), timeout()) -> status().
status(QPid, Timeout) ->
    gen_server:call(QPid, status, Timeout).

-spec index(solrq_id(), index_name(), bkey(), obj(), write_reason(), p()) -> ok.
index(QPid, Index, BKey, Obj, Reason, P) ->
    gen_server:call(QPid, {index, Index, {BKey, Obj, Reason, P}}, infinity).

-spec set_hwm(solrq_id(), HWM :: solrq_hwm()) ->
    {ok, OldHWM :: solrq_hwm()} | {error, bad_hwm_value}.
set_hwm(QPid, HWM) when HWM > 0 ->
    gen_server:call(QPid, {set_hwm, HWM});
set_hwm(_, _)  ->
    {error, bad_hwm_value}.

-spec set_index(solrq_id(), index_name(), solrq_batch_min(), solrq_batch_max(),
                solrq_batch_flush_interval()) -> 
                    {ok, {solrq_batch_min(),
                         solrq_batch_max(),
                         solrq_batch_flush_interval()}}.
set_index(QPid, Index, Min, Max, DelayMax)
    when Min > 0, Min =< Max, DelayMax >= 0 orelse DelayMax == infinity ->
    gen_server:call(QPid, {set_index, Index, Min, Max, DelayMax});
set_index(_, _, _, _, _) ->
    {error, bad_index_params}.

-spec set_purge_strategy(solrq_id(), purge_strategy()) ->
    {ok, OldPurgeStrategy :: purge_strategy()} | {error, bad_purge_strategy}.
set_purge_strategy(QPid, PurgeStrategy)
    when    PurgeStrategy == ?PURGE_NONE
    orelse  PurgeStrategy == ?PURGE_ONE
    orelse  PurgeStrategy == ?PURGE_IDX
    orelse  PurgeStrategy == ?PURGE_ALL ->
    gen_server:call(QPid, {set_purge_strategy, PurgeStrategy});
set_purge_strategy(_QPid, _PurgeStrategy) ->
    {error, bad_purge_strategy}.

-spec reload_appenv(solrq_id()) -> ok.
reload_appenv(QPid) ->
    gen_server:call(QPid, reload_appenv).

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
-spec blown_fuse(solrq_id(), index_name()) -> ok.
blown_fuse(QPid, Index) ->
    gen_server:cast(QPid, {blown_fuse, Index}).

%% @doc Signal to the solrq that a fuse has healed for the the specified index.
-spec healed_fuse(solrq_id(), index_name()) -> ok.
healed_fuse(QPid, Index) ->
    gen_server:cast(QPid, {healed_fuse, Index}).

%% @doc return the sum of the length of all queues in each indexq
-spec get_hwm(solrq_id()) -> HWM :: non_neg_integer().
get_hwm(QPid) ->
    gen_server:call(QPid, get_hwm).

%% @doc return the sum of the length of all queues in each indexq
-spec all_queue_len(solrq_id()) -> non_neg_integer().
all_queue_len(QPid) ->
    gen_server:call(QPid, all_queue_len).

%%%===================================================================
%%% solrq/helper interface
%%%===================================================================
-spec request_batch(solrq_id(), index_name(), solrq_helper_id()) -> ok.
request_batch(QPid, Index, HPid) ->
    gen_server:cast(QPid, {request_batch, Index, HPid}).


-spec batch_complete(
    solrq_id(),
    index_name(),
    {NumDelivered :: non_neg_integer(), ok} |
    {NumDelivered :: non_neg_integer(),
     {retry, [Undelivered :: solrq_message()]}}) -> ok.
batch_complete(QPid, Index, Message) ->
    gen_server:cast(QPid, {batch_complete, Index, Message}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
    {ok, read_appenv(#state{})} .

handle_call({index, Index, E}, From, State) ->
    ?PULSE_DEBUG("index.  State: ~p~n", [debug_state(State)]),
    State2 = inc_qlen_and_maybe_unblock_vnode(From, State),
    IndexQ = enqueue(E, get_indexq(Index, State2)),
    IndexQ2 = maybe_request_worker(Index, IndexQ),
    IndexQ3 = maybe_start_timer(Index, IndexQ2),
    NewState = update_indexq(Index, IndexQ3, State2),
    ?PULSE_DEBUG("index.  NewState: ~p~n", [debug_state(NewState)]),
    {noreply, NewState};
handle_call(status, _From, #state{} = State) ->
    {reply, internal_status(State), State};
handle_call({set_hwm, NewHWM}, _From, #state{queue_hwm = OldHWM} = State) ->
    {reply, {ok, OldHWM}, maybe_unblock_vnodes(State#state{queue_hwm = NewHWM})};
handle_call(get_hwm, _From, #state{queue_hwm = HWM} = State) ->
    {reply, HWM, State};
handle_call({set_index, Index, Min, Max, DelayMS}, _From, State) ->
    IndexQ = get_indexq(Index, State),
    IndexQ2 = maybe_request_worker(Index,
                                   IndexQ#indexq{batch_min = Min,
                                                 batch_max = Max,
                                                 delayms_max = DelayMS}),
    OldParams = {IndexQ#indexq.batch_min,
                 IndexQ#indexq.batch_max,
                 IndexQ#indexq.delayms_max},
    {reply, {ok, OldParams}, update_indexq(Index, IndexQ2, State)};
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
handle_cast({request_batch, Index, HPid}, State) ->
    State2 = send_entries(HPid, Index, State),
    {noreply, State2};

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
handle_cast({drain, DPid, Token, Partition}, #state{indexqs = IndexQs} = State) ->
    ?PULSE_DEBUG("drain{~p=DPid, ~p=Token, ~p=Partition}.  State: ~p~n", [debug_state(State), DPid, Token, Partition]),
    {Remaining, NewIndexQs} = dict:fold(
        fun(Index, IndexQ0, {RemainingAccum, IndexQsAccum}) ->
            IndexQ = partition(IndexQ0, Partition),
            case {IndexQ#indexq.queue_len, IndexQ#indexq.in_flight_len} of
                {0, 0} ->
                    {RemainingAccum, dict:store(Index, IndexQ#indexq{draining = wait_for_drain_complete}, IndexQsAccum)};
                {0, _InFlightLen} ->
                    {[Index | RemainingAccum], dict:store(Index, IndexQ#indexq{draining = true}, IndexQsAccum)};
                _ ->
                    {[Index | RemainingAccum], drain(Index, IndexQ, IndexQsAccum)}
            end
        end,
        {[], IndexQs},
        IndexQs
    ),
    NewState =
        case Remaining of
            [] ->
                yz_solrq_drain_fsm:drain_complete(DPid, Token),
                State;
            _ ->
                State#state{indexqs = NewIndexQs, drain_info = {DPid, Token, Remaining}}

        end,
    ?PULSE_DEBUG("drain.  NewState: ~p~n", [debug_state(NewState)]),
    {noreply, NewState};


%% @doc The fuse for the specified index has blown.
handle_cast({blown_fuse, Index}, State) ->
    {noreply, handle_blown_fuse(Index, State)};

%%
%% @doc Clear the fuse_blown state on the IndexQ associated with the supplied Index
%%      Resume any batches that may need to proceed.
%% @end
%%
handle_cast({healed_fuse, Index}, State) ->
    IndexQ = get_indexq(Index, State),
    NewIndexQ = maybe_request_worker(Index, maybe_start_timer(Index, IndexQ#indexq{fuse_blown = false})),
    {noreply, update_indexq(Index, NewIndexQ, State)};

%%
%% @doc     Handle the batch_complete message.
%%
%%          The batch_complete message is sent via the batch_complete/3 function
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
handle_cast({batch_complete, Index, {NumDelivered, Result}}, #state{all_queue_len = AQL} = State) ->
    ?PULSE_DEBUG("batch_complete.  State: ~p~n", [debug_state(State)]),
    IndexQ = get_indexq(Index, State),
    State1 = handle_batch(Index, IndexQ#indexq{pending_helper = false, in_flight_len = 0}, Result, State),
    NewState = maybe_unblock_vnodes(State1#state{all_queue_len = AQL - NumDelivered}),
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
handle_cast(drain_complete, #state{indexqs = IndexQs} = State) ->
    NewIndexQs = dict:fold(
        fun(Index, #indexq{queue = Queue, queue_len = QueueLen, aux_queue = AuxQueue} = IndexQ, IndexQAccum) ->
            NewIndexQ = IndexQ#indexq{
                queue = queue:join(AuxQueue, Queue),
                queue_len = QueueLen + queue:len(AuxQueue),
                aux_queue = queue:new(),
                draining = false
            },
            dict:store(Index, maybe_start_timer(Index, maybe_request_worker(Index, NewIndexQ)), IndexQAccum)
        end,
        dict:new(),
        IndexQs
    ),
    {noreply, State#state{indexqs = NewIndexQs, drain_info = undefined}}.


%% @doc Timer has fired - request a worker.
handle_info({timeout, _TimerRef, {flush, Index}}, State) ->
    case find_indexq(Index, State) of
        undefined ->
            {noreply, State};
        IndexQ ->
            case timer_running(IndexQ) of
                true ->
                    {noreply,
                     flush(Index,
                           IndexQ#indexq{timer_ref = undefined},
                           State)};
                false -> % out of date
                    {noreply, State}
            end
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Check if timer (erlang:start_timer/3) ref is defined.
-spec timer_running(#indexq{}) -> boolean().
timer_running(#indexq{timer_ref = undefined}) -> false;
timer_running(_) -> true.

%%
%% @doc
%% A drain is not in progress.  There are two cases to consider:
%%   1. The result is ok; This means all batched messages were delivered.
%%   2. The solrq_helper returned some undelivered messages; pre-pend these to the queue
%%      for this index, and request a new worker, if we are over the requested minimum.
%% @end
%%
handle_batch(
    Index,
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
    IndexQ2 = maybe_request_worker(Index, IndexQ1),
    IndexQ3 = maybe_start_timer(Index, IndexQ2),
    update_indexq(Index, IndexQ3, State);

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
%%       the queue and request another worker.
%%
%% If there are no remaining indexqs to drain, then send the drain FSM
%% a drain_complete message for this solrq.
%% @end
%%
handle_batch(
  Index,
  #indexq{queue_len = QueueLen, draining = _Draining, batch_start = T1} = IndexQ,
  Result,
  #state{drain_info = DrainInfo} = State) ->
    {DPid, Token, Remaining} = DrainInfo,
    {NewRemaining, NewIndexQ} =
        case Result of
            ok ->
                case QueueLen of
                    0 ->
                        yz_stat:batch_end(?YZ_TIME_ELAPSED(T1)),
                        {
                            lists:delete(Index, Remaining),
                            IndexQ#indexq{
                                draining = wait_for_drain_complete,
                                batch_start = undefined
                            }
                        };
                    _ ->
                        {Remaining, request_worker(Index, IndexQ)}
                end;
            {retry, Undelivered} ->
                {Remaining, requeue_undelivered(Undelivered, request_worker(Index, IndexQ))}
        end,
    %%
    %% If there are no remaining indexqs to be flushed, send the drain FSM
    %% a drain complete for this solrq instance.
    %%
    case NewRemaining of
        [] ->
            yz_solrq_drain_fsm:drain_complete(DPid, Token);
        _ ->
            ok
    end,
    update_indexq(
        Index,
        maybe_start_timer(Index, NewIndexQ),
        State#state{drain_info = {DPid, Token, NewRemaining}}
    ).

partition(IndexQ, undefined) ->
    IndexQ;
partition(IndexQ, Partition) ->
    {NewQueue, NewAuxQueue} = lists:partition(
        fun({_BKey, _Obj, _Reason, P}) ->
            P =:= Partition
        end,
        queue:to_list(IndexQ#indexq.queue)
    ),
    IndexQ#indexq{
        queue = queue:from_list(NewQueue),
        queue_len = length(NewQueue),
        aux_queue = queue:join(queue:from_list(NewAuxQueue), IndexQ#indexq.aux_queue)
    }.

drain(Index, IndexQ, IndexQs) ->
    %% NB. A drain request may occur while a helper is pending
    %% (and hence while a batch may be "in flight" to Solr).  If
    %% so, then no worker will be requested.  However, the draining
    %% flag will be set on the indexq.  Hence, when the drain_complete
    %% message is received, if there are any pending messages still in
    %% the queue, a worker will be requested at that time, and the indexq
    %% will remain in the draining state, the result being that the
    %% indexq will eventually get drained, once the current in-flight
    %% batch completes.
    IndexQ2 = request_worker(Index, IndexQ),
    dict:store(Index, IndexQ2#indexq{draining = true}, IndexQs).

-spec internal_status(#state{}) -> status().
internal_status(#state{indexqs = IndexQs} = State) ->
    [{F, V} || {F, V} <- ?REC_PAIRS(state, State), F /= indexqs] ++
        [{indexqs,
          lists:sort(
            dict:fold(
              fun(Index, IndexQ, Acc) ->
                      [{Index, [{F, V} || {F, V} <- ?REC_PAIRS(indexq, IndexQ),
                                         F /= queue]} | Acc]
              end, [], IndexQs))}].

%% @doc Increment the aggregated queue length and see if the vnode needs to be
%%      stalled.
inc_qlen_and_maybe_unblock_vnode(From, #state{all_queue_len = AQL,
                                              pending_vnodes = PendingVnodes}
                                 = State) ->
    State2 = State#state{all_queue_len = AQL + 1},
    case over_hwm(State2) of
        true ->
            State3 = maybe_purge_blown_indices(State2),
            case over_hwm(State3) of
                true ->
                    yz_stat:blocked_vnode(From),
                    State3#state{pending_vnodes = [From | PendingVnodes]};
                false ->
                    gen_server:reply(From, ok),
                    State3
            end;
        false ->
            gen_server:reply(From, ok),
            State2
    end.

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
flush(Index, IndexQ, State) ->
    IndexQ2 = request_worker(Index, IndexQ),
    update_indexq(Index, IndexQ2, State).

%% @doc handle a blown fuse by setting the fuse_blown flag,
%%      purging any data if required, and unblocking any vnodes
%%      if required.
handle_blown_fuse(Index, #state{purge_strategy=PBIStrategy} = State) ->
    IndexQ = get_indexq(Index, State),
    State1 = update_indexq(Index, IndexQ#indexq{fuse_blown = true}, State),
    State2 = maybe_purge([{Index, IndexQ}], PBIStrategy, State1),
    maybe_unblock_vnodes(State2).

%% @doc purge entries depending on purge strategy if we are over the HWM
maybe_purge(Indices, PBIStrategy, State) ->
    case over_hwm(State) of
        true ->
            purge(Indices, PBIStrategy, State);
        _ ->
            State
    end.

%% @doc Return true if queue is over the high water mark
over_hwm(#state{all_queue_len = L, queue_hwm = HWM}) ->
    L > HWM.

maybe_purge_blown_indices(#state{purge_strategy=?PURGE_NONE} = State) ->
    State;
maybe_purge_blown_indices(#state{indexqs=IndexQs,
                                 purge_strategy=PBIStrategy}
                          = State) ->
    BlownIndices = [{Index, IndexQ} ||
                       {Index,
                        #indexq{fuse_blown=true,
                                queue_len=QueueLen,
                                aux_queue=AuxQueue} = IndexQ} <-
                           dict:to_list(IndexQs),
                       QueueLen > 0 orelse queue:len(AuxQueue) > 0
                   ],
    case BlownIndices of
        [] ->
            State;
        _ ->
            purge(BlownIndices, PBIStrategy, State)
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
-spec purge([{index_name(), #indexq{}}], purge_strategy(), #state{}) ->
    #state{}.
purge(BlownIndices, PurgeStrategy, State) ->
    {NewState, NumPurged} = purge_internal(BlownIndices, PurgeStrategy, State),
    case NumPurged of
        0 ->
            ok;
        _ ->
            yz_stat:hwm_purged(NumPurged)
    end,
    NewState.

-spec purge_internal([{index_name(), #indexq{}}], purge_strategy(), #state{}) ->
                            {#state{}, TotalNumPurged :: non_neg_integer()}.
purge_internal(BlownIndices, ?PURGE_ALL, State) ->
    lists:foldl(fun purge_idx/2, {State, 0}, BlownIndices);
purge_internal(BlownIndices, PurgeStrategy, State) ->
    I = random:uniform(length(BlownIndices)),
    BlownIndex = lists:nth(I, BlownIndices),
    purge_idx(BlownIndex, PurgeStrategy, State).

-spec purge_idx(BlownIndex :: {index_name(), #indexq{}},
                {#state{}, NumPurgedAccum :: non_neg_integer()}) ->
                       {#state{}, TotalNumPurged :: non_neg_integer()}.
purge_idx(BlownIndex, {State, PurgeNumAccum}) ->
    {NewState, NumPurged} = purge_idx(BlownIndex, ?PURGE_IDX, State),
    {NewState, PurgeNumAccum + NumPurged}.

-spec purge_idx(BlownIndex :: {index_name(), #indexq{}},
                purge_strategy(),
                #state{}) -> {#state{}, NumPurged :: non_neg_integer()}.
purge_idx(_, ?PURGE_NONE, State) ->
    {State, 0};
purge_idx(
  {Index,
   #indexq{queue_len=QueueLen, queue=OldQueue, aux_queue=OldAuxQueue}=IndexQ
  },
  ?PURGE_ONE,
  #state{all_queue_len=AllQueueLen} = State) ->
    OldAuxQueueLen = queue:len(OldAuxQueue),
    NewIndexQ =
        case OldAuxQueueLen > 0 of
            true ->
                {{value, _Item}, NewAuxQueue} = queue:out(OldAuxQueue),
                IndexQ#indexq{aux_queue=NewAuxQueue};
            _ ->
                {{value, _Item}, NewQueue} = queue:out(OldQueue),
                IndexQ#indexq{queue=NewQueue, queue_len=QueueLen - 1}
        end,
    ?DEBUG("Removing item from queue because we have hit the high"
           " watermark and the fuse is blown for index ~p, using"
           " purge_strategy ~p", [Index, ?PURGE_ONE]),
    {update_indexq(
       Index, NewIndexQ, State#state{all_queue_len=AllQueueLen - 1}),
     1};
purge_idx({Index, #indexq{queue_len=QueueLen, aux_queue=AuxQueue}=IndexQ},
          ?PURGE_IDX,
          #state{all_queue_len=AllQueueLen} = State) ->
    AuxQueueLen = queue:len(AuxQueue),
    NumPurged = QueueLen + AuxQueueLen,
    NewIndexQ = IndexQ#indexq{queue=queue:new(), aux_queue=queue:new(),
                              queue_len=0},
    ?DEBUG("Removing index-queue because we have hit the high"
           " watermark and the fuse is blown for index ~p, using"
           " purge_strategy ~p", [Index, ?PURGE_IDX]),
    {update_indexq(
       Index, NewIndexQ, State#state{all_queue_len = AllQueueLen - NumPurged}),
     NumPurged}.

%% @doc Request a worker to pull the queue
maybe_request_worker(Index, #indexq{batch_min = Min} = IndexQ) ->
    maybe_request_worker(Index, Min, IndexQ).

%% @doc Request a worker to pulll the queue with a provided minimum,
%%      as long as one has not already been requested.
maybe_request_worker(Index, Min, #indexq{pending_helper = false,
                                         fuse_blown = false,
                                         queue_len = L} = IndexQ) when L >= Min ->
    request_worker(Index, IndexQ);
maybe_request_worker(_Index, _Min, IndexQ) ->
    IndexQ.

%% @doc Notify the solrq workers the index is ready to be pulled.
request_worker(Index, #indexq{pending_helper = false, fuse_blown = false} = IndexQ) ->
    yz_solrq_helper:index_ready(Index, self()),
    IndexQ#indexq{pending_helper = true};
request_worker(_Index, IndexQ) ->
    IndexQ.

%% @doc Send a batch of entries, reply to any blocked vnodes and
%%      return updated state
send_entries(HPid, Index, State) ->
    IndexQ = get_indexq(Index, State),
    #indexq{batch_max = BatchMax} = IndexQ,
    {Batch, _BatchLen, IndexQ2} = get_batch(IndexQ),
    yz_solrq_helper:index_batch(HPid, Index, BatchMax, self(), Batch),
    case IndexQ2#indexq.queue_len of
        0 ->
            % all the messages have been sent
            update_indexq(Index, IndexQ2, State);
        _ ->
            % may be another full batch
            IndexQ3 = maybe_request_worker(Index, IndexQ2),
            % if entries left, restart timer if batch not full
            IndexQ4 = maybe_start_timer(Index, IndexQ3),
            update_indexq(Index, IndexQ4, State)
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
maybe_unblock_vnodes(#state{pending_vnodes = []} = State) ->
    State;
maybe_unblock_vnodes(#state{pending_vnodes = PendingVnodes} = State) ->
    case over_hwm(State) of
        true ->
            State;
        _ ->
            _ = [gen_server:reply(From, unblocked) || From <- PendingVnodes],
            State#state{pending_vnodes = []}
    end.

maybe_start_timer(Index, #indexq{timer_ref = undefined, queue_len = L,
                                 pending_helper = false,
                                 fuse_blown = false,
                                 delayms_max = DelayMS} = IndexQ) when L > 0 ->
    TimerRef =
        case DelayMS of
            infinity ->
                lager:debug("Infinite delay, will not start timer and flush."),
                undefined;
            _ ->
                erlang:start_timer(DelayMS, self(), {flush, Index})
        end,
    IndexQ#indexq{timer_ref = TimerRef};
maybe_start_timer(_Index, IndexQ) ->
    IndexQ.

find_indexq(Index, #state{indexqs = IndexQs}) ->
    case dict:find(Index, IndexQs) of
        {ok, IndexQ} ->
            IndexQ;
        _ ->
            undefined
    end.

get_indexq(Index, #state{indexqs = IndexQs}) ->
    case dict:find(Index, IndexQs) of
        {ok, IndexQ} ->
            IndexQ;
        _ ->
            new_indexq()
    end.

set_new_index(Min, Max, DelayMS)
  when Min > 0, Min =< Max, DelayMS >= 0 orelse DelayMS == infinity ->
    #indexq{batch_min = Min,
            batch_max = Max,
            delayms_max = DelayMS};
set_new_index(_, _, _) ->
    #indexq{}.

new_indexq() ->
    BatchMin = app_helper:get_env(?YZ_APP_NAME, ?SOLRQ_BATCH_MIN, 1),
    BatchMax = app_helper:get_env(?YZ_APP_NAME, ?SOLRQ_BATCH_MAX, 100),
    DelayMS = app_helper:get_env(?YZ_APP_NAME, ?SOLRQ_BATCH_FLUSH_INTERVAL,
                                 1000),
    set_new_index(BatchMin, BatchMax, DelayMS).

update_indexq(Index, IndexQ, #state{indexqs = IndexQs} = State) ->
    State#state{indexqs = dict:store(Index, IndexQ, IndexQs)}.

%% @doc Read settings from the application environment
%% TODO: Update HWM for each Index when Ring-Resize occurrs
read_appenv(State) ->
    HWM = app_helper:get_env(?YZ_APP_NAME, ?SOLRQ_HWM, 10000),
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
        {indexqs, [{Index, debug_indexq(IndexQ)} || {Index, IndexQ} <- dict:to_list(State#state.indexqs)]},
        {all_queue_len, State#state.all_queue_len},
        {drain_info, State#state.drain_info}
    ].
-endif.
