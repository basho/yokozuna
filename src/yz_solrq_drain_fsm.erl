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
-export([start_link/1, cancel/0]).

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

-define(SERVER, ?MODULE).

-record(state, {
    tokens,
    drain_complete_callback
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(fun(() -> ok)) -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link(DrainCompleteCallback) ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, DrainCompleteCallback, []).

%%
%% @doc TODO
%%
drain_complete(DPid, Token) ->
    gen_fsm:send_event(DPid, {drain_complete, Token}).

start_prepare() ->
    gen_fsm:send_event(?MODULE, start).

cancel() ->
    case catch gen_fsm:sync_send_all_state_event(?MODULE, cancel, 5000) of
        {'EXIT', {noproc, _}} ->
            no_proc;
        {'EXIT', {timeout, _}} ->
            timeout;
        Any ->
            Any
    end.

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init(DrainCompleteCallback) ->
    {ok, prepare, #state{drain_complete_callback = DrainCompleteCallback}}.


%% TODO doc
prepare(start, State) ->
    SolrqIds = yz_solrq_sup:solrq_names(),
    Tokens = [yz_solrq:drain(SolrqId) || SolrqId <- SolrqIds],
    {next_state, wait, State#state{tokens = Tokens}}.

%% TODO doc
wait({drain_complete, Token}, #state{tokens = Tokens, drain_complete_callback = DrainCompleteCallback} = State) ->
    Tokens2 = lists:delete(Token, Tokens),
    NewState = State#state{tokens = Tokens2},
    case Tokens2 of
        [] ->
            catch ok = DrainCompleteCallback(),
            [yz_solrq:drain_complete(Name) || Name <- yz_solrq_sup:solrq_names()],
            {stop, normal, NewState};
        _ ->
            {next_state, wait, NewState}
    end.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(cancel, _From, _StateName, State) ->
    [yz_solrq:cancel_drain(Name) || Name <- yz_solrq_sup:solrq_names()],
    {stop, normal, ok, State};

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
