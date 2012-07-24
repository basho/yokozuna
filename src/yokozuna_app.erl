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
    yz_index:add_to_ring(?YZ_INDEX),
    case yokozuna_sup:start_link() of
        {ok, Pid} ->
            register_app(),
            add_routes(wm_routes()),
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    ok.


%%%===================================================================
%%% Private
%%%===================================================================

add_routes(Routes) ->
    [webmachine_router:add_route(R) || R <- Routes].

register_app() ->
    Modules = [{vnode_module, yokozuna_vnode}],
    riak_core:register(yokozuna, Modules).

wm_routes() ->
    [{["search", index], yz_wm_search, []}].
