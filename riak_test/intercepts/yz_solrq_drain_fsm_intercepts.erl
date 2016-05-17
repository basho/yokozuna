%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
-module(yz_solrq_drain_fsm_intercepts).
-compile(export_all).

-define(M, yz_solrq_drain_fsm_orig).

%% Crash the start_prepare message, in order to property handle
%% failures in the drain manager handling code.
prepare_crash(start, State) ->
    {stop, {error, something_bad_happened}, State}.


%% Put a 5 second sleep in front of prepare.
prepare_sleep_5s(start, State) ->
    timer:sleep(5000),
    ?M:prepare_orig(start, State).


%% Put a 5 second sleep in front of prepare.
prepare_sleep_1s(start, State) ->
    timer:sleep(1000),
    ?M:prepare_orig(start, State).


%% restore the original
prepare_orig(start, State) ->
    ?M:prepare_orig(start, State).
