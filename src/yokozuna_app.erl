%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(yokozuna_app).
-behaviour(application).
-export([start/2, stop/1]). % prevent compile warnings
-compile(export_all).
-include("yokozuna.hrl").


%%%===================================================================
%%% Callbacks
%%%===================================================================

start(_StartType, _StartArgs) ->
    riak_core:wait_for_service(riak_kv),
    case yokozuna_sup:start_link() of
        {ok, Pid} ->
            Routes = yz_wm_search:routes() ++ yz_wm_extract:routes() ++ yz_wm_index:routes() ++ yz_wm_schema:routes(),
            yz_misc:add_routes(Routes),
            riak_core_node_watcher:service_up(yokozuna, Pid),
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    ok.
