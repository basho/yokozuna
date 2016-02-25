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
-module(yz_solrq).

-include("yokozuna.hrl").

-behavior(gen_server).

%% api
-export([start_link/1, status/1, index/5, set_hwm/2, set_index/5,
         reload_appenv/1, blown_fuse/2, healed_fuse/2, cancel_drain/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% solrq/helper interface
-export([request_batch/3, drain/1, drain_complete/1, batch_complete/3]).

%% TODO: Dynamically pulse_instrument.  See test/pulseh.erl
-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{gen_server, pulse_gen_server}]}).
-define(PULSE_DEBUG(S,F), pulse:format(S,F)).
-else.
-define(PULSE_DEBUG(S,F), ok).
-endif.

-type solrq_message()     :: tuple().  % {BKey, Docs, Reason, P}.
-type solrq_batch_min()   :: pos_integer().
-type solrq_batch_max()   :: pos_integer().
-type solrq_delayms_max() :: non_neg_integer()|infinity.

-record(
    indexq, {
        queue = queue:new()     :: yz_queue(),   % solrq_message()
        queue_len = 0           :: non_neg_integer(),
        href                    :: reference(),
        pending_helper = false  :: boolean(),
        batch_min = 1           :: solrq_batch_min(),
        batch_max = 100         :: solrq_batch_max(),
        delayms_max = 1000      :: solrq_delayms_max(),
        aux_queue = queue:new() :: yz_queue(),
        draining = false        :: boolean() | wait_for_drain_complete,
        fuse_blown = false      :: boolean(),
        in_flight_len = 0       :: non_neg_integer(),
        batch_start             :: timestamp()
    }
).

-record(
    state, {
        indexqs = dict:new()    :: yz_dict(),
        all_queue_len = 0       :: non_neg_integer(),
        queue_hwm = 1000        :: non_neg_integer(),
        pending_vnodes = []     :: [{pid(), atom()}],
        drain_info = undefined  :: {pid(), reference(), [solrq_message()]} | undefined,
        purge_blown_indices     :: boolean()
    }
).

%% TODO: Clean this up w/ proper type naming conventions.
-type internal_status() :: [{atom()|indexqs,
                             non_neg_integer() | [{pid(), atom()}] |
                             [{index_name(), {atom(), non_neg_integer() |
                                              reference() | boolean() |
                                              [{pid(), atom()}]}}]}].

-define(REC_PAIRS(Rec,Instance), lists:zip(record_info(fields, Rec),
                                           tl(tuple_to_list(Instance)))).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

-spec status(pid()) -> internal_status().
status(QPid) ->
    status(QPid, 5000).

-spec status(pid(), timeout()) -> internal_status().
status(QPid, Timeout) ->
    gen_server:call(QPid, status, Timeout).

-spec index(index_name(), bkey(), obj(), write_reason(), p()) -> ok.
index(Index, BKey, Obj, Reason, P) ->
    %% Hash on the index and partition to ensure updates to
    %% an index are serialized for all objects in the vnode.
    Hash = erlang:phash2({Index, BKey}),
    gen_server:call(yz_solrq_sup:queue_regname(Hash),
                    {index, Index, {BKey, Obj, Reason, P}}, infinity).


-spec set_hwm(pid, non_neg_integer()) -> #state{}.
set_hwm(QPid, HWM) ->
    gen_server:call(QPid, {set_hwm, HWM}).

-spec set_index(pid(), index_name(), solrq_batch_min(), solrq_batch_max(),
                solrq_delayms_max()) -> {ok, {solrq_batch_min(),
                                             solrq_batch_max(),
                                             solrq_delayms_max()}}.
set_index(QPid, Index, Min, Max, DelayMax) ->
    gen_server:call(QPid, {set_index, Index, Min, Max, DelayMax}).

-spec reload_appenv(pid()) -> ok.
reload_appenv(QPid) ->
    gen_server:call(QPid, reload_appenv).

-spec drain(atom()) -> reference().
drain(QPid) ->
    Token = make_ref(),
    gen_server:cast(QPid, {drain, self(), Token}),
    Token.

-spec drain_complete(atom()) -> ok.
drain_complete(QPid) ->
    gen_server:cast(QPid, drain_complete).

-spec cancel_drain(atom()) -> ok.
cancel_drain(QPid) ->
    gen_server:call(QPid, cancel_drain).

%% @doc Signal to the solrq that a fuse has blown for the the specified index.
-spec blown_fuse(pid(), index_name()) -> ok.
blown_fuse(QPid, Index) ->
    gen_server:cast(QPid, {blown_fuse, Index}).

%% @doc Signal to the solrq that a fuse has healed for the the specified index.
-spec healed_fuse(pid(), index_name()) -> ok.
healed_fuse(QPid, Index) ->
    gen_server:cast(QPid, {healed_fuse, Index}).

%%%===================================================================
%%% solrq/helper interface
%%%===================================================================
-spec request_batch(pid(), index_name(), pid()|atom()) -> ok.
request_batch(QPid, Index, HPid) ->
    gen_server:cast(QPid, {request_batch, Index, HPid}).


-spec batch_complete(
    pid(),
    index_name(),
    {NumDelivered :: non_neg_integer(), ok} | {NumDelivered :: non_neg_integer(), {retry, [Undelivered :: solrq_message()]}}) -> ok.
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
handle_call({set_index, Index, Min, Max, DelayMS}, _From, State) when
      Min > 0, Min =< Max, DelayMS >= 0 orelse DelayMS == infinity ->
    IndexQ = get_indexq(Index, State),
    IndexQ2 = maybe_request_worker(Index,
                                   IndexQ#indexq{batch_min = Min,
                                                 batch_max = Max,
                                                 delayms_max = DelayMS}),
    OldParams = {IndexQ#indexq.batch_min,
                 IndexQ#indexq.batch_max,
                 IndexQ#indexq.delayms_max},
    {reply, {ok, OldParams}, update_indexq(Index, IndexQ2, State)};
handle_call({set_index, Index, _Min, _Max, _DelayMS}, _From, State) ->
    IndexQ = get_indexq(Index, State),
    OldParams = {IndexQ#indexq.batch_min,
                 IndexQ#indexq.batch_max,
                 IndexQ#indexq.delayms_max},
    {reply, {{error, bad_index_params}, OldParams}, State};
handle_call(cancel_drain, _From, State) ->
    {noreply, NewState} = handle_cast(drain_complete, State),
    {reply, ok, NewState};
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
handle_cast({drain, DPid, Token}, #state{indexqs = IndexQs} = State) ->
    ?PULSE_DEBUG("drain.  State: ~p~n", [debug_state(State)]),
    {Remaining, NewIndexQs} = dict:fold(
        fun(Index, IndexQ, {RemainingAccum, IndexQsAccum}) ->
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

%%
%% @doc Set the fuse_blown state on the IndexQ associated with the supplied Index
%%
handle_cast({blown_fuse, Index}, State) ->
    IndexQ = get_indexq(Index, State),
    {noreply, update_indexq(Index, IndexQ#indexq{fuse_blown = true}, State)};

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
                queue = queue:join(Queue, AuxQueue),
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


handle_info({flush, Index, HRef}, State) -> % timer has fired - request a worker
    case find_indexq(Index, State) of
        undefined ->
            {noreply, State};
        IndexQ ->
            case IndexQ#indexq.href of
                HRef ->
                    {noreply, flush(Index, IndexQ#indexq{href = undefined}, State)};
                _ -> % out of date
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


%%
%% @doc
%% A drain is not in progress.  There are two cases to consider:
%%   1. The result is ok; This means all batched messages were delivered; Reduce the
%%      all_queue_len by the number of messages delivered.
%%   2. The solrq_helper returned some undelivered messages; pre-pend these to the queue
%%      for this index, and request a new worker, if we are over the requested minimum.
%%      Reduce the all_queue_len by the number of messages delivered.
%%
%% Since the ACL has been adjusted, unblock any vnodes that might be waiting.
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
                IndexQ0;
            {retry, Undelivered} ->
                requeue_undelivered(Undelivered, IndexQ0)
        end,
    IndexQ2 = maybe_request_worker(Index, IndexQ1),
    IndexQ3 = maybe_start_timer(Index, IndexQ2),
    update_indexq(Index, IndexQ3, State);

%%
%% @doc
%% This queue is being drained.  There are two cases to consider:
%%    1. The batch succeeded.  In this case, move any data we
%%       have accumulated in aux_queue to the main queue,
%%       and remove ourselves from the list
%%       of remaining indices that need to be flushed.
%%    2. The batch did not succeed.  In this case, we got back a list
%%       of undelivered messages.  Put the undelivered messages back onto
%%       the queue and request another worker.
%%
%% Since the ACL has been adjusted, unblock any vnodes that might be waiting.
%% @end
%%
handle_batch(
    Index,
    #indexq{queue_len = QueueLen, draining = true, batch_start = T1} = IndexQ,
    Result,
    #state{drain_info = DrainInfo} = State
) ->
    {DPid, Token, Remaining} = DrainInfo,
    {NewRemaining, NewIndexQ} =
        case Result of
            ok ->
                case QueueLen of
                    0 ->
                        yz_stat:batch_end(?YZ_TIME_ELAPSED(T1)),
                        {
                            lists:delete(Index, Remaining),
                            IndexQ#indexq{draining = wait_for_drain_complete}
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
    NewDrainInfo = case NewRemaining of
        [] ->
            yz_solrq_drain_fsm:drain_complete(DPid, Token),
            undefined;
        _ ->
            {DPid, Token, NewRemaining}
    end,
    update_indexq(
        Index,
        maybe_start_timer(Index, NewIndexQ),
        State#state{drain_info = NewDrainInfo}
    ).



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

-spec internal_status(#state{}) -> internal_status().
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

%% @doc Return true if queue is over the high water mark
over_hwm(#state{all_queue_len = L, queue_hwm = HWM}) ->
    L > HWM.

maybe_purge_blown_indices(#state{purge_blown_indices = false} = State) ->
    State;
maybe_purge_blown_indices(#state{indexqs=IndexQs} = State) ->
    BlownIndices = [
        {Index, IndexQ} ||
            {Index, #indexq{fuse_blown=true, queue_len=QueueLen} = IndexQ} <- dict:to_list(IndexQs),
            QueueLen > 0
    ],
    case BlownIndices of
        [] ->
            State;
        _ ->
            I = random:uniform(length(BlownIndices)),
            {Index, #indexq{queue=Queue, queue_len=QueueLen} = IndexQ} = lists:nth(I, BlownIndices),
            {{value, _Item}, NewQueue} = queue:out(Queue),
            NewIndexQ = IndexQ#indexq{queue=NewQueue, queue_len=QueueLen - 1},
            ?WARN("Removing item from queue because we have hit the high water mark and the fuse is blown for index ~p.", [Index]),
            update_indexq(Index, NewIndexQ, State)
    end.


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

%% @doc Get up to batch_max entries and reset the pending worker/timer href
get_batch(#indexq{queue = Q, queue_len = L, batch_max = Max, draining = Draining} = IndexQ) ->
    {BatchQ, RestQ} =
        case Draining of
            true ->
                {Q, queue:new()};
            _ ->
                queue:split(min(L,Max), Q)
        end,
    Batch = queue:to_list(BatchQ),
    BatchLen = length(Batch),
    IndexQ2 = IndexQ#indexq{queue = RestQ, queue_len = L - BatchLen,
                            href = undefined, in_flight_len = BatchLen,
                            batch_start = os:timestamp()},
    {Batch, BatchLen, IndexQ2}.


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

maybe_start_timer(Index, #indexq{href = undefined, queue_len = L,
                                 pending_helper = false,
                                 fuse_blown = false,
                                 delayms_max = DelayMS} = IndexQ) when L > 0 ->
    HRef = make_ref(),
    case DelayMS of
        infinity ->
            lager:debug("Infinite delay, will not start timer and flush.");
        _ ->
            erlang:send_after(DelayMS, self(), {flush, Index, HRef})
    end,
    IndexQ#indexq{href = HRef};
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
  when Min =< Max, DelayMS >= 0 orelse DelayMS == infinity ->
    #indexq{batch_min = Min,
            batch_max = Max,
            delayms_max = DelayMS};
set_new_index(_, _, _) ->
    #indexq{}.

new_indexq() ->
    BatchMin = app_helper:get_env(?YZ_APP_NAME, solrq_batch_min, 1),
    BatchMax = app_helper:get_env(?YZ_APP_NAME, solrq_batch_max, 100),
    DelayMS = app_helper:get_env(?YZ_APP_NAME, solrq_delayms_max, 1000),
    set_new_index(BatchMin, BatchMax, DelayMS).

update_indexq(Index, IndexQ, #state{indexqs = IndexQs} = State) ->
    State#state{indexqs = dict:store(Index, IndexQ, IndexQs)}.

% TODO re-add once we have a reaping strategy
%delete_indexq(Index, #state{indexqs = IndexQs} = State) ->
%    State#state{indexqs = dict:erase(Index, IndexQs)}.

%% @doc Read settings from the application environment
read_appenv(State) ->
    HWM = app_helper:get_env(?YZ_APP_NAME, solrq_queue_hwm, 10000),
    PBI = application:get_env(?YZ_APP_NAME, purge_blown_indices, true),
    State#state{queue_hwm = HWM, purge_blown_indices=PBI}.

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
