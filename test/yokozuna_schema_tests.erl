%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
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

-module(yokozuna_schema_tests).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

-define(DEFAULT_DISABLED_JOB_CLASSES, [
]).
-define(DEFAULT_ENABLED_JOB_CLASSES, [
    {yokozuna, query}
]).
-define(JOB_CLASS_CONFIG_KEY,   [riak_core, job_accept_class]).

%% basic schema test will check to make sure that all defaults from
%% the schema make it into the generated app.config
basic_schema_test() ->
     %% The defaults are defined in ../priv/yokozuna.schema. it is the
     %% file under test.
    Config = cuttlefish_unit:generate_templated_config(
               "../priv/yokozuna.schema", [], context(), predefined_schema()),

    cuttlefish_unit:assert_config(Config, "yokozuna.enabled", false),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_startup_wait", 30),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_port", 12345),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_jmx_port", 54321),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_jvm_opts",
                                  "-d64 -Xms1g -Xmx1g -XX:+UseStringCache -XX:+UseCompressedOops"),
    cuttlefish_unit:assert_config(Config, "yokozuna.anti_entropy_data_dir",
                                  "./data/yolo/yz_anti_entropy"),
    cuttlefish_unit:assert_config(Config, "yokozuna.root_dir", "./data/yolo/yz"),
    cuttlefish_unit:assert_config(Config, "yokozuna.temp_dir",
                                  "./data/yolo/yz_temp"),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_request_timeout",
                                  60000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_ed_request_timeout",
                                  60000),
    cuttlefish_unit:assert_config(Config, "yokozuna.anti_entropy", {on, []}),
    cuttlefish_unit:assert_config(Config, "yokozuna.err_thresh_fail_count",
                                  3),
    cuttlefish_unit:assert_config(Config,
                                  "yokozuna.err_thresh_fail_interval",
                                  5000),
    cuttlefish_unit:assert_config(Config,
                                  "yokozuna.err_thresh_reset_interval",
                                  30000),
    cuttlefish_unit:assert_config(Config, "yokozuna.fuse_ctx",
                                  async_dirty),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_hwm_purge_strategy",
                                  purge_one),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_min", 10),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_max", 500),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_flush_interval",
                                  500),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_hwm", 1000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_drain_timeout", 60000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_drain_cancel_timeout", 5000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_drain_enable", true),
    cuttlefish_unit:assert_config(Config, "yokozuna.ibrowse_max_sessions", 100),
    cuttlefish_unit:assert_config(Config, "yokozuna.ibrowse_max_pipeline_size", 1),
    cuttlefish_unit:assert_config(Config, "yokozuna.aae_throttle_enabled", true),
    cuttlefish_unit:assert_not_configured(Config, "yokozuna.aae_throttle_limits"),
    cuttlefish_unit:assert_config(Config, "yokozuna.enable_dist_query", true),
    cuttlefish_unit:assert_config(Config, "riak_kv.index_module", yokozuna),
    ok.

override_schema_test() ->
    %% Conf represents the riak.conf file that would be read in by
    %% cuttlefish.  this proplists is what would be output by the
    %% conf_parse module
    Conf = [
            {["search"], on},
            {["search", "solr", "start_timeout"], "1m"},
            {["search", "solr", "port"], 5678},
            {["search", "solr", "jmx_port"], 8765},
            {["search", "solr", "jvm_options"], "-Xmx10G"},
            {["search", "anti_entropy", "data_dir"], "/data/aae/search"},
            {["search", "root_dir"], "/some/other/volume"},
            {["search", "temp_dir"], "/some/other/volume_temp"},
            {["search", "solr", "request_timeout"], "90s"},
            {["search", "solr", "ed_request_timeout"], "90s"},
            {["search", "anti_entropy"], "passive"},
            {["search", "index", "error_threshold", "failure_count"], 5},
            {["search", "index", "error_threshold", "failure_interval"], "10s"},
            {["search", "index", "error_threshold", "reset_interval"], "60s"},
            {["search", "fuse_context"], "sync"},
            {["search", "queue", "batch", "minimum"], 100},
            {["search", "queue", "batch", "maximum"], 10000},
            {["search", "queue", "batch", "flush_interval"], infinity},
            {["search", "queue", "high_watermark"], 100000},
            {["search", "queue", "high_watermark", "purge_strategy"], "purge_index"},
            {["search", "queue", "drain", "enable"], "off"},
            {["search", "queue", "drain", "timeout"], "2m"},
            {["search", "queue", "drain", "cancel", "timeout"], "10ms"},
            {["search", "ibrowse_max_sessions"], 101},
            {["search", "ibrowse_max_pipeline_size"], 11},
            {["search", "anti_entropy", "throttle"], off},
            {["search", "anti_entropy", "throttle", "tier1", "solrq_queue_length"], 0},
            {["search", "anti_entropy", "throttle", "tier1", "delay"], "1d"},
            {["search", "anti_entropy", "throttle", "tier2", "solrq_queue_length"], 11},
            {["search", "anti_entropy", "throttle", "tier2", "delay"], "10d"},
            {["search", "dist_query"], "off"}
           ],
    Config = cuttlefish_unit:generate_templated_config(
               "../priv/yokozuna.schema", Conf, context(), predefined_schema()),

    cuttlefish_unit:assert_config(Config, "yokozuna.enabled", true),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_startup_wait", 60),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_port", 5678),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_jmx_port", 8765),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_jvm_opts", "-Xmx10G"),
    cuttlefish_unit:assert_config(Config, "yokozuna.anti_entropy_data_dir",
                                  "/data/aae/search"),
    cuttlefish_unit:assert_config(Config, "yokozuna.root_dir", "/some/other/volume"),
    cuttlefish_unit:assert_config(Config, "yokozuna.temp_dir", "/some/other/volume_temp"),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_request_timeout", 90000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_ed_request_timeout", 90000),
    cuttlefish_unit:assert_config(Config, "yokozuna.anti_entropy", {off, []}),
    cuttlefish_unit:assert_config(Config, "yokozuna.err_thresh_fail_count",
                                  5),
    cuttlefish_unit:assert_config(Config,
                                  "yokozuna.err_thresh_fail_interval",
                                  10000),
    cuttlefish_unit:assert_config(Config,
                                  "yokozuna.err_thresh_reset_interval",
                                  60000),
    cuttlefish_unit:assert_config(Config, "yokozuna.fuse_ctx",
                                  sync),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_hwm_purge_strategy",
        purge_index),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_min", 100),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_max", 10000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_flush_interval",
                                  infinity),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_hwm", 100000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_drain_timeout",
                                  120000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_drain_cancel_timeout",
                                  10),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_drain_enable", false),
    cuttlefish_unit:assert_config(Config, "yokozuna.ibrowse_max_sessions", 101),
    cuttlefish_unit:assert_config(Config, "yokozuna.ibrowse_max_pipeline_size", 11),
    cuttlefish_unit:assert_config(Config, "yokozuna.aae_throttle_enabled",
                                  false),
    cuttlefish_unit:assert_config(Config, "yokozuna.aae_throttle_limits",
                                  [{-1, 86400000}, {10, 864000000}]),
    cuttlefish_unit:assert_config(Config, "yokozuna.enable_dist_query", false),
    ok.

validations_test() ->
    %% Conf represents the riak.conf file that would be read in by
    %% cuttlefish.  this proplists is what would be output by the
    %% conf_parse module
    Conf = [
            {["search"], on},
            {["search", "index", "error_threshold", "failure_count"], 0},
            {["search", "queue", "batch", "minimum"], -1},
            {["search", "queue", "high_watermark"], -1},
            {["search", "queue", "batch", "maximum"], -10}
    ],
    Config = cuttlefish_unit:generate_templated_config(
               "../priv/yokozuna.schema", Conf, context(), predefined_schema()),
    cuttlefish_unit:assert_error_in_phase(Config, validation),
    {error, validation, {errorlist, ListOfErrors0}} = Config,
    ListOfErrors1 = [Error || {error, {validation, Error}} <- ListOfErrors0],
    ListOfErrors2 = lists:keysort(1, ListOfErrors1),
    ?assertEqual([{"search.queue.batch.maximum",
                   "must be a positive integer > 0"},
                  {"search.queue.batch.minimum",
                   "must be a positive integer > 0"},
                  {"search.queue.high_watermark",
                   "must be an integer >= 0"}],
                 ListOfErrors2),
    ok.

job_class_defaults_test() ->
    confirm_enabled_job_classes(riak_core_schema()).

confirm_enabled_job_classes({true, RCSchema}) when erlang:is_list(RCSchema) ->
    Config = cuttlefish_unit:generate_templated_config(
        [RCSchema, "../priv/yokozuna.schema"], [],
        riak_core_schema_tests:context() ++ context()),
    Enabled = config_value(?JOB_CLASS_CONFIG_KEY, Config),

    [?assertEqual(lists:member(Class, Enabled), true)
        || Class <- ?DEFAULT_ENABLED_JOB_CLASSES],

    [?assertEqual(lists:member(Class, Enabled), false)
        || Class <- ?DEFAULT_DISABLED_JOB_CLASSES],

    ok;

% If riak_core is not present, or eunit hasn't been run there, the necessary
% schema and/or beam file won't be found. If we fail the test buildbot won't
% pass because the riak_core .eunit files haven't been built.
confirm_enabled_job_classes({error, enoent}) ->
    ?debugMsg("Supporting riak_core components not present,"
        " skipping job_class_defaults_test").

%% this context() represents the substitution variables that rebar
%% will use during the build process.  yokozuna's schema file is
%% written with some {{mustache_vars}} for substitution during
%% packaging cuttlefish doesn't have a great time parsing those, so we
%% perform the substitutions first, because that's how it would work
%% in real life.
context() ->
    [
        {yz_solr_port, 12345}, %% The port an idiot would open on their luggage
        {yz_solr_jmx_port, 54321}
    ].

%% This predefined schema covers riak_kv's dependency on
%% platform_data_dir
predefined_schema() ->
    Mapping = cuttlefish_mapping:parse({mapping,
                                        "platform_data_dir",
                                        "riak_core.platform_data_dir", [
                                            {default, "./data/yolo"},
                                            {datatype, directory}
                                       ]}),
    {[], [Mapping], []}.

config_value([Key], Config) ->
    proplists:get_value(Key, Config);
config_value([Key | Keys], Config) ->
    case proplists:get_value(Key, Config) of
        undefined = Val ->
            Val;
        Vals ->
            config_value(Keys, Vals)
    end.

%% Ensure that the riak_core_schema_tests module is loaded and return the
%% path of the riak_core.schema file.
riak_core_schema() ->
    riak_core_schema(riak_core_dir()).
riak_core_schema({RCDir, Schema}) when erlang:is_list(RCDir) ->
    case code:ensure_loaded(riak_core_schema_tests) of
        {module, _} ->
            {true, Schema};
        _ ->
            Search = filename:join([RCDir, "**", "riak_core_schema_tests.beam"]),
            case filelib:wildcard(Search) of
                [Beam | _] ->
                    case code:load_abs(filename:rootname(Beam)) of
                        {module, _} ->
                            {true, Schema};
                        Error ->
                            Error
                    end;
                [] ->
                    {error, enoent}
            end
    end;
riak_core_schema(Error) ->
    Error.

riak_core_dir() ->
    TryDeps = case os:getenv("REBAR_DEPS_DIR") of
        false ->
            ["../deps", "../.."];
        Dir ->
            [Dir, "../deps"]
    end,
    riak_core_dir(TryDeps).
riak_core_dir([Deps | TryDeps]) ->
    RCDir   = filename:join(Deps, "riak_core"),
    Schema  = filename:join([RCDir, "priv", "riak_core.schema"]),
    case filelib:is_regular(Schema) of
        true ->
            {RCDir, Schema};
        _ ->
            riak_core_dir(TryDeps)
    end;
riak_core_dir([]) ->
    {error, enoent}.
