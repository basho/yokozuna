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
-module(yz_solrq_drain_mgr_intercepts).
-compile(export_all).

-define(M, yz_solrq_drain_mgr_orig).

%% Add some sleep before the drain begins, in order to introduce
%% a race condition in YZ AAE which we test for and accomodate.
%% This is primarily needed to bring about the race on MacOS (and
%% possibly other BSD systems); linux scheduling seems more aggressive.
delay_drain(Params) ->
    timer:sleep(100),
    ?M:drain_orig(Params).


