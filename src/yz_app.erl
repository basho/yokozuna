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

-module(yz_app).
-behaviour(application).
-export([start/2, stop/1]). % prevent compile warnings
-compile(export_all).
-include("yokozuna.hrl").

%% 27 is message type rpbsearchqueryreq
%% 28 is message type rpbsearchqueryresp
-define(QUERY_SERVICES, [{yz_pb_search, 27, 28}]).
-define(ADMIN_SERVICES, [{yz_pb_admin, 54, 57}]).

%%%===================================================================
%%% Callbacks
%%%===================================================================

start(_StartType, _StartArgs) ->
    riak_core:wait_for_service(riak_kv),
    Enabled = ?YZ_ENABLED,
    case yz_sup:start_link(Enabled) of
	{ok, Pid} ->
	    maybe_setup(Enabled, Pid),
	    {ok, Pid};
	Error ->
	    Error
    end.

stop(_State) ->
    ok = riak_api_pb_service:deregister(?QUERY_SERVICES),
    ok = riak_api_pb_service:deregister(?ADMIN_SERVICES),
    ok.

maybe_setup(false, _) ->
    ok;
maybe_setup(true, SupPid) ->
    Routes = yz_wm_search:routes() ++ yz_wm_extract:routes() ++
	yz_wm_index:routes() ++ yz_wm_schema:routes(),
    yz_misc:add_routes(Routes),
    maybe_register_pb(?QUERY_SERVICES),
    maybe_register_pb(?ADMIN_SERVICES),
    riak_core_node_watcher:service_up(yokozuna, SupPid),
    ok.

%% @doc Conditionally register PB service IFF Riak Search is not
%%      enabled.
-spec maybe_register_pb(list()) -> ok.
maybe_register_pb(Services) ->
    case yz_misc:is_riak_search_enabled() of
        false ->
            ok = riak_api_pb_service:register(Services);
        true ->
            ok
    end.

