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

-behaviour(gen_fsm).

%% API
-export([start_link/0, start_link/1, cancel/1]).

%% gen_fsm callbacks
-export([init/1,
    handle_event/3,
    handle_sync_event/4,
    handle_info/3,
    terminate/3,
    code_change/4]).

-export([start_prepare/0, prepare/2, wait/2, drain_complete/2]).

-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{gen_fsm, pulse_gen_fsm}]}).
-endif.

-include("yokozuna.hrl").
-define(SERVER, ?MODULE).

-record(state, {
    tokens,
    exchange_fsm_pid,
    yz_index_hashtree_update_params,
    partition,
    time_start
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
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, Params, []).

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
    gen_fsm:send_event(DPid, {drain_complete, Token}).

%% @doc Start draining.  This operation will send a start message to this
%% FSM with a start message, which in turn will initiate drains on all of
%% the solrqs.
%%
%% NB. This function is typically called from the drain manager.
%% @end
%%
-spec start_prepare() -> no_proc | timeout | term().
start_prepare() ->
    gen_fsm:send_event(?SERVER, start).

%% @doc Cancel a drain.  This operation will result in sending a cancel
%% message to each of the solrqs, putting them back into a batching state.
%% @end
%%
-spec cancel(non_neg_integer()) -> ok | no_proc | timeout.
cancel(Timeout) ->
    try
        gen_fsm:sync_send_all_state_event(?SERVER, cancel, Timeout)
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

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init(Params) ->
    {ok, prepare, #state{
        exchange_fsm_pid = proplists:get_value(?EXCHANGE_FSM_PID, Params),
        yz_index_hashtree_update_params = proplists:get_value(?YZ_INDEX_HASHTREE_PARAMS, Params),
        partition = proplists:get_value(?DRAIN_PARTITION, Params)
    }}.


%% @doc We receive a start message in the prepare state when start_prepare/0 is called.
%% This will initiate a drain on all the solrqs.  This gives us a list of references (tokens)
%% we wait for while in the wait state, the state in which we immediate move into.
%% @end
%%
prepare(start, #state{partition = P} = State) ->
    SolrqIds = yz_solrq:solrq_worker_names(),
    TS = os:timestamp(),
    Tokens = [yz_solrq_worker:drain(SolrqId, P) || SolrqId <- SolrqIds],
    {next_state, wait, State#state{tokens = Tokens, time_start=TS}}.

%% @doc While in the wait state, we wait for drain_complete messages with accompanying
%% tokens.  When we have received all of the tokens, we are done, and the FSM terminates
%% normally.  Otherwise, we keep waiting.
%% @end
%%
wait({drain_complete, Token},
    #state{
        tokens = Tokens,
        exchange_fsm_pid = ExchangeFSMPid,
        yz_index_hashtree_update_params = YZIndexHashtreeUpdateParams,
        time_start = StartTS
    } = State) ->
    Tokens2 = lists:delete(Token, Tokens),
    NewState = State#state{tokens = Tokens2},
    case Tokens2 of
        [] ->
            yz_stat:drain_end(?YZ_TIME_ELAPSED(StartTS)),
            maybe_update_yz_index_hashtree(
                ExchangeFSMPid, YZIndexHashtreeUpdateParams
            ),
            [yz_solrq_worker:drain_complete(Name) || Name <- yz_solrq:solrq_worker_names()],
            {stop, normal, NewState};
        _ ->
            {next_state, wait, NewState}
    end.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(cancel, _From, _StateName, State) ->
    [yz_solrq_worker:cancel_drain(Name) || Name <- yz_solrq:solrq_worker_names()],
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

maybe_update_yz_index_hashtree(undefined, undefined) ->
    ok;
maybe_update_yz_index_hashtree(Pid, {YZTree, Index, IndexN}) ->
    yz_exchange_fsm:update_yz_index_hashtree(Pid, YZTree, Index, IndexN).
