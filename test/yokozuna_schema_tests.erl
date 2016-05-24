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
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_min", 1),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_max", 100),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_flush_interval",
                                  1000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_hwm", 10000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_worker_count", 10),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_helper_count", 10),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_drain_timeout", 60000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_drain_cancel_timeout", 5000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_drain_enable", true),
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
            {["search", "solr", "ed_request_timeout"], "90s"},
            {["search", "anti_entropy"], "passive"},
            {["search", "index", "error_threshold", "failure_count"], 5},
            {["search", "index", "error_threshold", "failure_interval"], "10s"},
            {["search", "index", "error_threshold", "reset_interval"], "60s"},
            {["search", "fuse_context"], "sync"},
            {["search", "queue", "batch", "minimum"], 10},
            {["search", "queue", "batch", "maximum"], 10000},
            {["search", "queue", "batch", "flush_interval"], infinity},
            {["search", "queue", "high_watermark"], 100000},
            {["search", "queue", "worker_count"], 5},
            {["search", "queue", "helper_count"], 20},
            {["search", "queue", "high_watermark", "purge_strategy"],
             "purge_all"},
            {["search", "queue", "drain", "enable"], "off"},
            {["search", "queue", "drain", "timeout"], "2m"},
            {["search", "queue", "drain", "cancel", "timeout"], "10ms"},
            {["search", "ibrowse_max_sessions"], 101},
            {["search", "ibrowse_max_pipeline_size"], 11}
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
        purge_all),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_min", 10),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_max", 10000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_batch_flush_interval",
                                  infinity),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_hwm", 100000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_worker_count", 5),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_helper_count", 20),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_drain_timeout",
                                  120000),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_drain_cancel_timeout",
                                  10),
    cuttlefish_unit:assert_config(Config, "yokozuna.solrq_drain_enable", false),
    cuttlefish_unit:assert_config(Config, "yokozuna.ibrowse_max_sessions", 101),
    cuttlefish_unit:assert_config(Config, "yokozuna.ibrowse_max_pipeline_size", 11),
    ok.

validations_test() ->
    %% Conf represents the riak.conf file that would be read in by
    %% cuttlefish.  this proplists is what would be output by the
    %% conf_parse module
    Conf = [
            {["search"], on},
            {["search", "index", "error_threshold", "failure_count"], 0},
            {["search", "queue", "batch", "minimum"], -1},
            {["search", "queue", "high_watermark"], 0},
            {["search", "queue", "batch", "maximum"], -10},
            {["search", "queue", "worker_count"], 0},
            {["search", "queue", "helper_count"], 20}
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
                   "must be a positive integer > 0"},
                  {"search.queue.worker_count",
                   "must be a positive integer > 0"}],
                 ListOfErrors2),
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
