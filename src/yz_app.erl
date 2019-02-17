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
-export([start/2, stop/1, prep_stop/1, components/0]).
-compile([export_all, nowarn_export_all]).      % @todo //lelf
-include("yokozuna.hrl").

%% 27 is message type rpbsearchqueryreq
%% 28 is message type rpbsearchqueryresp
-define(QUERY_SERVICES, [{yz_pb_search, 27, 28}]).
-define(ADMIN_SERVICES, [{yz_pb_admin, 54, 60}]).

%% NOTE: These default values are duplicated in yokozuna.schema.
-define(YZ_CONFIG_IBROWSE_MAX_SESSIONS_DEFAULT, 100).
-define(YZ_CONFIG_IBROWSE_MAX_PIPELINE_SIZE_DEFAULT, 1).

components() ->
    [index, search].

%%%===================================================================
%%% Callbacks
%%%===================================================================

start(_StartType, _StartArgs) ->

    %% Disable indexing/searching from KV until properly started,
    %% otherwise restarting under load generates large numbers
    %% of failures in yz_kv:index/3.
    disable_components(),

    %% Ensure that the KV service has fully loaded.
    riak_core:wait_for_service(riak_kv),


    initialize_atoms(),
    Enabled = ?YZ_ENABLED,
    case yz_sup:start_link(Enabled) of
        {ok, Pid} ->
            _ = application:set_env(ibrowse, inactivity_timeout, 600000),
            maybe_setup(Enabled),

            %% Now everything is started, permit usage by KV/query
            enable_components(),
            clique:register([yz_console]),
            {ok, Pid};
        Error ->
            Error
    end.

%% @doc Prepare to stop - called before the supervisor tree is shutdown
prep_stop(State) ->
    %% Wrap shutdown code with a try/catch - application carries on regardless,
    %% no error message or logging about the failure otherwise.
    try
        lager:info("Stopping application yokozuna.", []),
        riak_core_node_watcher:service_down(yokozuna),
        disable_components(),
        riak_api_pb_service:deregister(?QUERY_SERVICES),
        riak_api_pb_service:deregister(?ADMIN_SERVICES),
        yz_solrq_drain_mgr:drain()
    catch
        Type:Reason ->
            lager:error("Stopping application yokozuna - ~p:~p.",
                        [Type, Reason])
    end,
    State.

stop(_State) ->
    lager:info("Stopped application yokozuna.", []),
    ok.

%% @private
%%
%% @doc Enable all Yokozuna components.
-spec enable_components() -> ok.
enable_components() ->
    lists:foreach(fun yokozuna:enable/1, components()),
    ok.

%% @private
%%
%% @doc Disable all Yokozuna components.
-spec disable_components() -> ok.
disable_components() ->
    lists:foreach(fun yokozuna:disable/1, components()),
    ok.

%% @private
%%
%% @doc Initialize atoms required outside of Yokozuna. E.g. the KV
%% bucket properties use the `list_to_existing_atom/1' call. If a user
%% sets the `search_index' property then this atom needs to
%% exist. These atoms should always be initialized since the user can
%% set these properties regardless if Yokozuna is enabled.
-spec initialize_atoms() -> ok.
initialize_atoms() ->
    %% I had to run `list_to_existing_atom' so compiler wouldn't
    %% optimize out `list_to_atom'.
    _ = list_to_atom("search_index"),
    search_index = list_to_existing_atom("search_index"),
    ok.

maybe_setup(false) ->
    ok;
maybe_setup(true) ->
    Routes = yz_wm_search:routes() ++ yz_wm_extract:routes() ++
        yz_wm_index:routes() ++ yz_wm_schema:routes(),
    ok = yz_events:add_guarded_handler(yz_events, []),
    yz_fuse:setup(),
    setup_stats(),
    ok = riak_core_capability:register(?YZ_CAPS_CMD_EXTRACTORS, [true, false],
                                       false),
    ok = riak_core_capability:register(
           ?YZ_CAPS_HANDLE_LEGACY_DEFAULT_BUCKET_TYPE_AAE,
           [v1, v0],
           v0),
    ok = riak_core:register(yokozuna, [{bucket_validator, yz_bucket_validator}]),
    ok = riak_core:register(search, [{permissions, ['query',admin]}]),
    ok = yz_schema:setup_schema_bucket(),
    ok = set_ibrowse_config(),
    yz_misc:add_routes(Routes),
    register_pb(),
    ok.

%% @doc Register PB service
-spec register_pb() -> ok.
register_pb() ->
    ok = riak_api_pb_service:register(?QUERY_SERVICES),
    ok = riak_api_pb_service:register(?ADMIN_SERVICES).

%% @private
%%
%% @doc Determine if direct stats are being used or not.  If not setup
%% a new sidejob stat resource.  Finally, register Yokozuna stats.
-spec setup_stats() -> ok.
setup_stats() ->
    case app_helper:get_env(riak_kv, direct_stats, false) of
        true -> ok;
        false -> sidejob:new_resource(yz_stat_sj, yz_stat_worker, 10000)
    end,
    ok = riak_core:register(yokozuna, [{stat_mod, yz_stat}]).

set_ibrowse_config() ->
    Config = [{?YZ_SOLR_MAX_SESSIONS,
               app_helper:get_env(?YZ_APP_NAME,
                                  ?YZ_CONFIG_IBROWSE_MAX_SESSIONS,
                                  ?YZ_CONFIG_IBROWSE_MAX_SESSIONS_DEFAULT)},
              {?YZ_SOLR_MAX_PIPELINE_SIZE,
               app_helper:get_env(?YZ_APP_NAME,
                                  ?YZ_CONFIG_IBROWSE_MAX_PIPELINE_SIZE,
                                  ?YZ_CONFIG_IBROWSE_MAX_PIPELINE_SIZE_DEFAULT)}
             ],
    yz_solr:set_ibrowse_config(Config).
