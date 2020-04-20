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
-compile(nowarn_export_all).
-compile(export_all).

-define(M, yz_solrq_drain_fsm_orig).

%% Crash the start_prepare message, in order to property handle
%% failures in the drain manager handling code.
prepare_crash(start, State) ->
    {stop, {error, something_bad_happened}, State}.

%% Put a 1 second sleep in front of resume_workers.
resume_workers_sleep_1s(Pid) ->
    timer:sleep(1000),
    ?M:resume_workers_orig(Pid).

%% restore the original prepare
prepare_orig(start, State) ->
    ?M:prepare_orig(start, State).

%% restore the original resume_workers
resume_workers_orig(Pid) ->
    ?M:resume_workers_orig(Pid).

%% Timeout on a cancel, full stop
cancel_timeout(_Pid, _CancelTimeout) ->
    lager:log(info, self(), "Intercepting cancel/2 and returning timeout"),
    timeout.

%% restore the original cancel
cancel_orig(Pid, CancelTimeout) ->
    ?M:cancel_orig(Pid, CancelTimeout).
