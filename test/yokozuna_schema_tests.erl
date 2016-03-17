-module(yokozuna_schema_tests).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

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
    cuttlefish_unit:assert_config(Config, "yokozuna.temp_dir", "./data/yolo/yz_temp"),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_request_timeout", 60000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_request_ed_timeout", 60000),
    cuttlefish_unit:assert_config(Config, "yokozuna.anti_entropy", {on, []}),
    cuttlefish_unit:assert_config(Config, "yokozuna.melt_attempts", 3),
    cuttlefish_unit:assert_config(Config, "yokozuna.melt_time_window", 5000),
    cuttlefish_unit:assert_config(Config, "yokozuna.melt_reset_refresh", 30000),
    cuttlefish_unit:assert_config(Config, "yokozuna.fuse_context", async_dirty),
    cuttlefish_unit:assert_config(Config, "yokozuna.purge_blown_indices", true),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_min", 1),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_max", 100),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_delayms_max", 1000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_queue_hwm", 10000),
    cuttlefish_unit:assert_config(Config, "yokozuna.num_solrq", 10),
    cuttlefish_unit:assert_config(Config, "yokozuna.num_solrq_helpers", 10),
    cuttlefish_unit:assert_config(Config, "yokozuna.ibrowse_max_sessions", 100),
    cuttlefish_unit:assert_config(Config, "yokozuna.ibrowse_max_pipeline_size", 1),
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
            {["search", "solr", "request_ed_timeout"], "90s"},
            {["search", "anti_entropy"], "passive"},
            {["search", "melt_attempts"], 5},
            {["search", "melt_time_window"], "10s"},
            {["search", "melt_reset_refresh"], "60s"},
            {["search", "fuse_context"], "sync"},
            {["search", "purge_blown_indices"], "off"},
            {["search", "solrq_batch_min"], 10},
            {["search", "solrq_batch_max"], 10000},
            {["search", "solrq_delayms_max"], infinity},
            {["search", "solrq_queue_hwm"], 100000},
            {["search", "ibrowse_max_sessions"], 101},
            {["search", "ibrowse_max_pipeline_size"], 11},
            {["search", "num_solrq"], 5},
            {["search", "num_solrq_helpers"], 20}
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
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_request_ed_timeout", 90000),
    cuttlefish_unit:assert_config(Config, "yokozuna.anti_entropy", {off, []}),
    cuttlefish_unit:assert_config(Config, "yokozuna.melt_attempts", 5),
    cuttlefish_unit:assert_config(Config, "yokozuna.melt_time_window", 10000),
    cuttlefish_unit:assert_config(Config, "yokozuna.melt_reset_refresh", 60000),
    cuttlefish_unit:assert_config(Config, "yokozuna.fuse_context", sync),
    cuttlefish_unit:assert_config(Config, "yokozuna.purge_blown_indices", false),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_min", 10),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_max", 10000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_delayms_max", infinity),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_queue_hwm", 100000),
    cuttlefish_unit:assert_config(Config, "yokozuna.ibrowse_max_sessions", 101),
    cuttlefish_unit:assert_config(Config, "yokozuna.ibrowse_max_pipeline_size", 11),
    cuttlefish_unit:assert_config(Config, "yokozuna.num_solrq", 5),
    cuttlefish_unit:assert_config(Config, "yokozuna.num_solrq_helpers", 20),
    ok.

validations_test() ->
    %% Conf represents the riak.conf file that would be read in by
    %% cuttlefish.  this proplists is what would be output by the
    %% conf_parse module
    Conf = [
            {["search"], on},
            {["search", "melt_attempts"], 0},
            {["search", "solrq_batch_min"], -1},
            {["search", "solrq_batch_max"], -10},
            {["search", "num_solrq"], 0},
            {["search", "num_solrq_helpers"], 20}
    ],
    Config = cuttlefish_unit:generate_templated_config(
               "../priv/yokozuna.schema", Conf, context(), predefined_schema()),
    cuttlefish_unit:assert_error_in_phase(Config, validation),
    ?assertEqual({error,validation, {errorlist,
                  [{error,
                    {validation,
                     {"search.solrq_batch_min",
                      "must be a positive integer > 0"}}},
                  {error,
                    {validation,
                     {"search.solrq_batch_max",
                      "must be a positive integer > 0"}}},
                   {error,
                    {validation,
                     {"search.num_solrq",
                      "must be a positive integer > 0"}}}]}},
                 Config),
    ok.

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
