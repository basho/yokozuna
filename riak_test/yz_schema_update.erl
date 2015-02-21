-module(yz_schema_update).
-compile(export_all).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(NUM_KEYS, 10000).
-define(BUCKET_TYPE, <<"data">>).
-define(INDEX, <<"fruit_aae">>).
-define(BUCKET, {?BUCKET_TYPE, ?INDEX}).
-define(REPAIR_MFA, {yz_exchange_fsm, repair, 2}).
-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 16}
          ]},
         {riak_kv,
          [
           %% allow AAE to build trees and exchange rapidly
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 4}
          ]},
         {yokozuna,
          [
           {enabled, true},
           {anti_entropy_tick, 1000}
          ]}
        ]).


confirm() ->
    case yz_rt:bb_driver_setup() of
        {ok, YZBenchDir} ->
            random:seed(now()),
            Cluster = rt:build_cluster(4, ?CFG),
            PBConns = yz_rt:open_pb_conns(Cluster),
            PBConn = yz_rt:select_random(PBConns),
            setup_index(Cluster, PBConn, YZBenchDir),
            {0, _} = yz_rt:load_data(Cluster, ?BUCKET, YZBenchDir, ?NUM_KEYS),
            lager:info("Verify data was indexed"),
            verify_num_match(Cluster, ?NUM_KEYS),
            %% Wait for a full round of exchange and then get total repair count
            TS1 = erlang:now(),
            yz_rt:wait_for_full_exchange_round(Cluster, TS1),
            %% TODO: Add a new field to the schema
            
            pass;
        {error, bb_driver_build_failed} ->
            lager:info("Failed to build the yokozuna basho_bench driver"
                       " required for this test"),
            fail
    end.

read_schema(YZBenchDir) ->
    Path = filename:join([YZBenchDir, "schemas", "fruit_schema.xml"]),
    {ok, RawSchema} = file:read_file(Path),
    RawSchema.

verify_num_match(Cluster, Num) ->
    F = fun(Node) ->
                HP = hd(yz_rt:host_entries(rt:connection_info([Node]))),
                yz_rt:search_expect(HP, ?INDEX, "text", "apricot", Num)
        end,
    yz_rt:wait_until(Cluster, F).

setup_index(Cluster, PBConn, YZBenchDir) ->
    Node = yz_rt:select_random(Cluster),
    RawSchema = read_schema(YZBenchDir),
    ok = yz_rt:store_schema(PBConn, ?INDEX, RawSchema),
    ok = yz_rt:wait_for_schema(Cluster, ?INDEX, RawSchema),
    ok = yz_rt:create_bucket_type(Node, ?BUCKET_TYPE),
    ok = yz_rt:create_index(Node, ?INDEX, ?INDEX),
    ok = yz_rt:wait_for_index(Cluster, ?INDEX),
    ok = yz_rt:set_index(Node, ?BUCKET, ?INDEX).
