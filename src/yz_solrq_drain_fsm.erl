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
-module(yz_solrq_drain_fsm).

-behaviour(gen_fsm_compat).

%% API
-export([start_link/0, start_link/1, cancel/2]).

%% gen_fsm callbacks
-export([init/1,
    handle_event/3,
    handle_sync_event/4,
    handle_info/3,
    terminate/3,
    code_change/4]).

%% gen_fsm states
-export([
    prepare/2,
    wait_for_drain_complete/2,
    wait_for_snapshot_complete/2,
    wait_for_yz_hashtree_updated/2
]).

%% API
-export([
    start_prepare/1,
    drain_complete/2,
    drain_already_in_progress/2,
    resume_workers/1]).

-include("yokozuna.hrl").
-define(SERVER, ?MODULE).

-record(state, {
    tokens,
    exchange_fsm_pid,
    yz_index_hashtree_update_params,
    partition,
    time_start,
    owner_pid
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the drain FSM process.   Note that the drain FSM will immediately
%% go into the prepare state, but it will not start draining until start_prepate/0
%% is called.
%% @end
%%
-spec(start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    start_link([]).

%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%% @end
%%
-spec(start_link(drain_params()) -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link(Params) ->
    gen_fsm_compat:start_link(?MODULE, Params, []).

%% @doc Notify the drain FSM identified by DPid that the solrq associated
%% with the specified Token has completed draining.  Note that the solrq
%% will remain in the wait_for_drain_complete state until it receives a
%% drain_complete message back from the specified FSM.
%%
%% NB. This function is typically called from each solrq.
%% @end
%%
-spec drain_complete(pid(), reference()) -> ok.
drain_complete(DPid, Token) ->
    gen_fsm_compat:send_event(DPid, {drain_complete, Token}).

%% @doc Notify the drain FSM identified by DPid that the solrq associated
%% with the specified Token has an existing drain request in progress and
%% cannot perform another drain at this time.
%%
%% NB. This function is typically called from each solrq.
%% @end
%%
drain_already_in_progress(DPid, Token) ->
    gen_fsm_compat:send_event(DPid, {drain_already_in_progress, Token}).

%% @doc Start draining.  This operation will send a start message to this
%% FSM with a start message, which in turn will initiate drains on all of
%% the solrqs.
%%
%% NB. This function is typically called from the drain manager.
%% @end
%%
-spec start_prepare(pid()) -> no_proc | timeout | term().
start_prepare(DPid) ->
    gen_fsm_compat:send_event(DPid, start).

%% @doc Cancel a drain.  This operation will result in sending a cancel
%% message to each of the solrqs, putting them back into a batching state.
%% @end
%%
-spec cancel(pid(), non_neg_integer()) -> ok | no_proc | timeout.
cancel(DPid, Timeout) ->
    try
        gen_fsm_compat:sync_send_all_state_event(DPid, cancel, Timeout)
    catch
        _:{normal, _}  ->
            %% It's possible that the drain FSM terminates "naturally"
            %% between the time that we initiate a cancel and the time
            %% the cancel message is received and processed.
            ok;
        _:{no_proc, _}  ->
            no_proc;
        _:{timeout, _} ->
            timeout
    end.

%%
%% Resume workers, typically called from the callback supplied to
%% yz_index_hashtree.  We are declaring this one-liner as a public
%% API function in order to have an effective intecept in riak_test
%% C.f., yz_solrq_test:confirm_drain_fsm_timeout
%%
resume_workers(Pid) ->
    gen_fsm_compat:send_event(Pid, resume_workers).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init(Params) ->
    {ok, prepare, #state{
        exchange_fsm_pid = proplists:get_value(?EXCHANGE_FSM_PID, Params),
        yz_index_hashtree_update_params = proplists:get_value(?YZ_INDEX_HASHTREE_PARAMS, Params),
        partition = proplists:get_value(?DRAIN_PARTITION, Params),
        owner_pid = proplists:get_value(owner_pid, Params)
    }}.


%% @doc We receive a start message in the prepare state when start_prepare/0 is called.
%% This will initiate a drain on all the solrqs.  This gives us a list of references (tokens)
%% we wait for while in the wait state, the state in which we immediate move into.
%% @end
%%
prepare(start, #state{partition = P} = State) ->
    lager:debug("Solrq drain starting for partition ~p", [P]),
    SolrqIds = get_solrq_ids(P),
    maybe_send_drain_messages(P, SolrqIds, State).

%% @doc While in the wait state, we wait for drain_complete messages with accompanying
%% tokens.  When we have received all of the tokens, we are done, and the FSM terminates
%% normally.  Otherwise, we keep waiting.
%% @end
%%
wait_for_drain_complete({drain_complete, Token},
    #state{
        tokens = Tokens,
        exchange_fsm_pid = ExchangeFSMPid,
        yz_index_hashtree_update_params = YZIndexHashtreeUpdateParams,
        time_start = StartTS,
        partition = Partition
    } = State) ->
    Tokens2 = lists:delete(Token, Tokens),
    NewState = State#state{tokens = Tokens2},
    case Tokens2 of
        [] ->
            lager:debug("Solrq drain completed for all workers for partition ~p.  Resuming batching.", [Partition]),
            yz_stat:drain_end(?YZ_TIME_ELAPSED(StartTS)),
            Self = self(),
            %%
            %% This callback function will be called from within the
            %% yz_index_hashtree, after the hashtree we are exchanging
            %% is snapshotted, but before it is updated.  In this callback
            %% we let the workers know they can resume normal operations,
            %% and we inform ourself that workers have been resumed.
            %%
            SnapshotCompleteCallback = fun() ->
                resume_workers(Self)
            end,
            spawn_link(
                fun() ->
                    maybe_update_yz_index_hashtree(
                        ExchangeFSMPid, YZIndexHashtreeUpdateParams, SnapshotCompleteCallback
                    ),
                    gen_fsm_compat:send_event(Self, yz_hashtree_updated)
                end
            ),
            {next_state, wait_for_snapshot_complete, NewState};
        _ ->
            {next_state, wait_for_drain_complete, NewState}
    end;
%% If a drain is already in progress, but we are draining all queues,
%% Try again until the previous drain completes.
wait_for_drain_complete({drain_already_in_progress, Token, QPid},
        #state{partition=undefined,
            tokens = Tokens0}=State) ->
    NewToken = yz_solrq_worker:drain(QPid, undefined),
    Tokens1 = lists:delete(Token, Tokens0),
    Tokens2 = [NewToken | Tokens1],
    {next_state, wait_for_drain_complete, State#state{tokens = Tokens2}};

%% In the case of a single-partition drain, just stop and let
%% the calling process handle retries
wait_for_drain_complete({drain_already_in_progress, _Token}, State) ->
    {stop, overlapping_drain_requested, State}.

%% The workers have resumed normal operations.  Draining is now "complete",
%% but we need to wait for the yz_index_hashtree to update its inner hashes,
%% which can take some time.  In the meantime, notify the process waiting
%% for drains to complete
wait_for_snapshot_complete(resume_workers, #state{partition=Partition, owner_pid=OwnerPid} = State) ->
    lists:foreach(
        fun yz_solrq_worker:drain_complete/1,
        get_solrq_ids(Partition)
    ),
    notify_workers_resumed(OwnerPid),
    {next_state, wait_for_yz_hashtree_updated, State}.

wait_for_yz_hashtree_updated(yz_hashtree_updated, State) ->
    {stop, normal, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(cancel, _From, _StateName, #state{partition=Partition} = State) ->
    [yz_solrq_worker:cancel_drain(Name) || Name <- get_solrq_ids(Partition)],
    {stop, normal, ok, State};

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

notify_workers_resumed(OwnerPid) ->
    OwnerPid ! workers_resumed.

maybe_update_yz_index_hashtree(undefined, undefined, Callback) ->
    maybe_callback(Callback),
    ok;
maybe_update_yz_index_hashtree(Pid, {YZTree, Index, IndexN}, Callback) ->
    yz_exchange_fsm:update_yz_index_hashtree(Pid, YZTree, Index, IndexN, Callback).


%%
%% @doc There were no queues to drain for this index/partition
%% Just update the hashtree and stop rather than waiting
%% for messages that will never arrive.
maybe_send_drain_messages(_P, [],  #state{
    exchange_fsm_pid = ExchangeFSMPid,
    yz_index_hashtree_update_params = YZIndexHashtreeUpdateParams } = State) ->
    maybe_update_yz_index_hashtree(
        ExchangeFSMPid, YZIndexHashtreeUpdateParams, undefined
    ),
    {stop, normal, State};
%%
%% @doc one or more queues need to be drained - send
%% drain messages and move to wait state
maybe_send_drain_messages(P, SolrqIds, State) ->
    TS = os:timestamp(),
    Tokens = [yz_solrq_worker:drain(SolrqId, P) || SolrqId <- SolrqIds],
    {next_state, wait_for_drain_complete, State#state{tokens = Tokens, time_start = TS}}.

%%
%% @doc if partition is `undefined` then drain all queues.
%% This is used in the yz_solrq_drain_mgr:drain/0 case,
%% usually during yokozuna's `yz_app:prep_stop`
get_solrq_ids(undefined) ->
    yz_solrq:all_solrq_workers();
%%
%% @doc drain all queues for a particular partition, `P`.
get_solrq_ids(P) ->
    yz_solrq:solrq_workers_for_partition(P).

maybe_callback(undefined) ->
    ok;
maybe_callback(Callback) ->
    Callback().

