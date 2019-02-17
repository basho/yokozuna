%% -------------------------------------------------------------------
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
-module(yz_stat_worker).
-behavior(gen_server).
-compile([export_all, nowarn_export_all]).      % @todo //lelf

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("yokozuna.hrl").

%%%===================================================================
%%% API
%%%===================================================================

update(StatUpdate) ->
    sidejob:unbounded_cast(yz_stat_sj, {update, StatUpdate}).

%%%===================================================================
%%% Callbacks
%%%===================================================================

init([_Name]) ->
    {ok, no_state}.

handle_call(_Req, _From, S) ->
    ?WARN("Unexpected request received ~p", [_Req]),
    {reply, unexpected_req, S}.

handle_cast({update, StatUpdate}, S) ->
    yz_stat:perform_update(StatUpdate),
    {noreply, S};
handle_cast(_Req, S) ->
    ?WARN("Unexpected request received ~p", [_Req]),
    {noreply, S}.

handle_info(_Req, S) ->
    ?WARN("Unexpected request received ~p", [_Req]),
    {noreply, S}.

terminate(_, _) ->
    ok.

code_change(_, S, _) ->
    {ok, S}.
