-module(yokozuna_schema_tests).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

%% basic schema test will check to make sure that all defaults from
%% the schema make it into the generated app.config
basic_schema_test() ->
     %% The defaults are defined in ../priv/yokozuna.schema. it is the
     %% file under test.
    Config = cuttlefish_unit:generate_templated_config(
               "../priv/yokozuna.schema", [], context()),

    cuttlefish_unit:assert_config(Config, "yokozuna.enabled", false),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_port", 12345),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_jmx_port", 54321),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_jvm_args",
                                  "-Xms1g -Xmx1g -XX:+UseStringCache -XX:+UseCompressedOops"),
    cuttlefish_unit:assert_config(Config, "yokozuna.anti_entropy_data_dir",
                                  "./data/yolo/yz_anti_entropy"),
    cuttlefish_unit:assert_config(Config, "yokozuna.root_dir", "./data/yolo/yz"),
    ok.

override_schema_test() ->
    %% Conf represents the riak.conf file that would be read in by
    %% cuttlefish.  this proplists is what would be output by the
    %% conf_parse module
    Conf = [
            {["search"], on},
            {["search", "solr_port"], 5678},
            {["search", "solr_jmx_port"], 8765},
            {["search", "solr_jvm_args"], "-Xmx10G"},
            {["search", "anti_entropy", "data_dir"], "/data/aae/search"},
            {["search", "root_dir"], "/some/other/volume"}
    ],
    Config = cuttlefish_unit:generate_templated_config(
               "../priv/yokozuna.schema", Conf, context()),

    cuttlefish_unit:assert_config(Config, "yokozuna.enabled", true),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_port", 5678),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_jmx_port", 8765),
    cuttlefish_unit:assert_config(Config, "yokozuna.solr_jvm_args", "-Xmx10G"),
    cuttlefish_unit:assert_config(Config, "yokozuna.anti_entropy_data_dir",
                                  "/data/aae/search"),
    cuttlefish_unit:assert_config(Config, "yokozuna.root_dir", "/some/other/volume"),
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
        {yz_solr_jmx_port, 54321},
        {platform_data_dir, "./data/yolo"}
    ].
