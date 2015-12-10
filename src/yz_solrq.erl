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

%%TODO: Dynamically add pulse. NOT PRODUCTION
%% -compile([export_all,{parse_transform,pulse_instrument},{d,modargs}]).
%% -compile({pulse_replace_module, [{gen_server, pulse_gen_server}]}).

%% api
-export([start_link/1, status/1, index/5, set_hwm/2, set_index/5,
         reload_appenv/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% solrq/helper interface
-export([request_batch/3, drain/1, drain_complete/3]).

-record(
    indexq, {
        queue = queue:new()     :: yz_queue(),   % {BKey, Docs, Reason, P}
        queue_len = 0           :: non_neg_integer(),
        href                    :: reference(),
        pending_helper = false  :: boolean(),
        batch_min = 1           :: non_neg_integer(),
        batch_max = 100         :: non_neg_integer(),
        delayms_max = 100       :: non_neg_integer(),
        aux_queue = queue:new() :: yz_queue(),
        draining = false        :: boolean()
    }
).
-record(
    state, {
        indexqs = dict:new()    :: yz_dict(),
        all_queue_len = 0       :: non_neg_integer(),
        queue_hwm = 1000        :: non_neg_integer(),
        pending_vnodes = []     :: [{pid(), atom()}],
        drain_info = undefined  :: {pid(), reference()} | undefined
    }
).


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
    Hash = erlang:phash2({Index, P}),
    gen_server:call(yz_solrq_sup:queue_regname(Hash),
                    {index, Index, {BKey, Obj, Reason, P}}, infinity).


-spec set_hwm(pid, non_neg_integer()) -> #state{}.
set_hwm(QPid, HWM) ->
    gen_server:call(QPid, {set_hwm, HWM}).

-spec set_index(pid(), index_name(), non_neg_integer(), non_neg_integer(),
                non_neg_integer()) -> {ok, {non_neg_integer(),
                                           non_neg_integer(),
                                           non_neg_integer()}}.
set_index(QPid, Index, Min, Max, DelayMax) ->
    gen_server:call(QPid, {set_index, Index, Min, Max, DelayMax}).

-spec reload_appenv(pid()) -> ok.
reload_appenv(QPid) ->
    gen_server:call(QPid, reload_appenv).

-spec drain(pid()) -> reference().
drain(QPid) ->
    Token = make_ref(),
    gen_server:cast(QPid, {drain, self(), Token}),
    Token.

drain_complete(QPid, Index, Result) ->
    gen_server:cast(QPid, {drain_complete, Index, Result}).

%%%===================================================================
%%% solrq/helper interface
%%%===================================================================
-spec request_batch(pid(), index_name(), pid()|atom()) -> ok.
request_batch(QPid, Index, HPid) ->
    gen_server:cast(QPid, {request_batch, Index, HPid}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
    {ok, read_appenv(#state{})} .

handle_call({index, Index, E}, From, State) ->
    State2 = inc_qlen_and_maybe_unblock_vnode(From, State),
    IndexQ = enqueue(E, get_indexq(Index, State2)),
    IndexQ2 = maybe_request_worker(Index, IndexQ),
    IndexQ3 = maybe_start_timer(Index, IndexQ2),
    {noreply, update_indexq(Index, IndexQ3, State2)};
handle_call(status, _From, #state{} = State) ->
    {reply, internal_status(State), State};
handle_call({set_hwm, NewHWM}, _From, #state{queue_hwm = OldHWM} = State) ->
    {reply, {ok, OldHWM}, maybe_unblock_vnodes(State#state{queue_hwm = NewHWM})};
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
handle_call(reload_appenv, _From, State) ->
    {reply, ok, read_appenv(State)}.

handle_cast({request_batch, Index, HPid}, State) ->
    State2 = send_entries(HPid, Index, State),
    {noreply, maybe_unblock_vnodes(State2)};

handle_cast({drain, DPid, Token}, #state{indexqs = IndexQs} = State) ->
    NewIndexQs = dict:fold(
        fun(Index, IndexQ, IndexQsAccum) ->
            drain(Index, IndexQ, IndexQsAccum)
        end,
        IndexQs,
        IndexQs
    ),
    {noreply, State#state{indexqs = NewIndexQs, drain_info = {DPid, Token}}};

handle_cast({drain_complete, Index, Result}, #state{drain_info = DrainInfo} = State) ->
    case DrainInfo of
        undefined ->
            {noreply, State};
        {DPid, Token} ->
            IndexQ = get_indexq(Index, State),
            #indexq{queue = Queue, aux_queue = AuxQueue} = IndexQ2 = IndexQ#indexq{draining = false},
            %% assert the current queue is empty, because we would have drained the queue
            %%if we had gotten a drain_complete back from the helper.
            true = queue:is_empty(Queue),
            IndexQ3 =
                case Result of
                    ok ->
                        IndexQ2#indexq{
                            queue = AuxQueue,
                            queue_len = queue:len(AuxQueue),
                            aux_queue = queue:new()
                        };
                    {error, Undelivered} ->
                        NewQueue = queue:join(queue:from_list(Undelivered), AuxQueue),
                        IndexQ2#indexq{
                            queue = NewQueue,
                            queue_len = queue:len(NewQueue),
                            aux_queue = queue:new()
                        }
                end,
            yz_solrq_drain_fsm:drain_complete(DPid, Token),
            {noreply, update_indexq(Index, IndexQ3, State)}
    end.

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


drain(Index, IndexQ, IndexQs) ->
    % TODO What do we do if there is already a pending worker? (We might be okay)
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
            yz_stat:blocked_vnode(From),
            State2#state{pending_vnodes = [From | PendingVnodes]};
        false ->
            gen_server:reply(From, ok),
            State2
    end.

%% @doc Enqueue the entry and return updated state.
enqueue(E, #indexq{queue = Q, queue_len = L, draining = Draining} = IndexQ) ->
    case Draining of
        true ->
            IndexQ#indexq{aux_queue = queue:in(E, Q)};
        false ->
            IndexQ#indexq{queue = queue:in(E, Q), queue_len = L + 1}
    end.

%% @doc Trigger a flush and return state
flush(Index, IndexQ, State) ->
    IndexQ2 = request_worker(Index, IndexQ),
    update_indexq(Index, IndexQ2, State).

%% @doc Return true if queue is over the high water mark
over_hwm(#state{all_queue_len = L, queue_hwm = HWM}) ->
    L > HWM.

%% @doc Request a worker to pull the queue
maybe_request_worker(Index, #indexq{batch_min = Min} = IndexQ) ->
    maybe_request_worker(Index, Min, IndexQ).

%% @doc Request a worker to pulll the queue with a provided minimum,
%%      as long as one has not already been requested.
maybe_request_worker(Index, Min, #indexq{pending_helper = false,
                                         queue_len = L} = IndexQ) when L >= Min ->
    request_worker(Index, IndexQ);
maybe_request_worker(_Index, _Min, IndexQ) ->
    IndexQ.

%% @doc Notify the solrq workers the index is ready to be pulled.
request_worker(Index, #indexq{pending_helper = false} = IndexQ) ->
    Hash = erlang:phash2({Index, self()}),
    yz_solrq_helper:index_ready(Hash, Index, self()),
    IndexQ#indexq{pending_helper = true};
request_worker(_Index, IndexQ) ->
    IndexQ.


%% @doc Send a batch of entries, reply to any blocked vnodes and
%%      return updated state
send_entries(HPid, Index, #state{all_queue_len = AQL} = State) ->
    IndexQ = get_indexq(Index, State),
    #indexq{batch_max = BatchMax} = IndexQ,
    {Batch, BatchLen, IndexQ2} = get_batch(IndexQ),
    yz_solrq_helper:index_batch(HPid, Index, BatchMax, self(), Batch),
    State2 = State#state{all_queue_len = AQL - BatchLen},
    case IndexQ2#indexq.queue_len of
        0 ->
            delete_indexq(Index, State2);
        _ ->
            % may be another full batch
            IndexQ3 = maybe_request_worker(Index, IndexQ2),
            % if entries left, restart timer if batch not full
            IndexQ4 = maybe_start_timer(Index, IndexQ3),
            update_indexq(Index, IndexQ4, State2)
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
                            pending_helper = false, href = undefined},
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
                                 delayms_max = DelayMS} = IndexQ) when L > 0 ->
    HRef = make_ref(),
    erlang:send_after(DelayMS, self(), {flush, Index, HRef}),
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

new_indexq() ->
    #indexq{batch_min = app_helper:get_env(yokozuna, solrq_batch_min, 1),
            batch_max = app_helper:get_env(yokozuna, solrq_batch_max, 100),
            delayms_max = app_helper:get_env(yokozuna, solrq_delayms_max, 1000)}.

update_indexq(Index, IndexQ, #state{indexqs = IndexQs} = State) ->
    State#state{indexqs = dict:store(Index, IndexQ, IndexQs)}.

delete_indexq(Index, #state{indexqs = IndexQs} = State) ->
    State#state{indexqs = dict:erase(Index, IndexQs)}.

%% @doc Read settings from the application environment
read_appenv(State) ->
    HWM = app_helper:get_env(yokozuna, solrq_queue_hwm, 10000),
    State#state{queue_hwm = HWM}.
