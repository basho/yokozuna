-module(aae_test).
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
    YZBenchDir = rt_config:get(yz_dir) ++ "/misc/bench",
    random:seed(now()),
    Cluster = rt:build_cluster(4, ?CFG),
    PBConns = yz_rt:open_pb_conns(Cluster),
    PBConn = yz_rt:select_random(PBConns),
    setup_index(Cluster, PBConn, YZBenchDir),
    {0, _} = yz_rt:load_data(Cluster, ?BUCKET, YZBenchDir, ?NUM_KEYS),
    lager:info("Verify data was indexed"),
    verify_num_match(Cluster, ?NUM_KEYS),
    %% Wait for a full round of exchange and then get total repair
    %% count.  Need to do this because setting AAE so agressive means
    %% that the Solr soft-commit could race with AAE and thus repair
    %% may happen while loading the data.  By getting the count before
    %% deleting keys it can be subtracted from the total count later
    %% to verify that the exact number of repairs was made given the
    %% number of keys deleted.
    TS1 = erlang:now(),
    yz_rt:wait_for_full_exchange_round(Cluster, TS1),
    RepairCountBefore = get_cluster_repair_count(Cluster),
    yz_rt:count_calls(Cluster, ?REPAIR_MFA),
    Keys = yz_rt:random_keys(?NUM_KEYS),
    {DelKeys, _ChangeKeys} = lists:split(length(Keys) div 2, Keys),
    lager:info("Deleting ~p keys", [length(DelKeys)]),
    [delete_key_in_solr(Cluster, ?INDEX, K) || K <- DelKeys],
    lager:info("Verify Solr indexes missing"),
    verify_num_match(Cluster, ?NUM_KEYS - length(DelKeys)),
    lager:info("Clear trees so AAE will notice missing indexes"),
    [ok = rpc:call(Node, yz_entropy_mgr, clear_trees, []) || Node <- Cluster],
    lager:info("Wait for all trees to re-build"),
    %% Before exchange of a partition can take place the KV and
    %% Yokozuna hashtree must be built.  Wait for all trees before
    %% checking that Solr indexes are repaired.
    TS2 = erlang:now(),
    yz_rt:wait_for_full_exchange_round(Cluster, TS2),
    lager:info("Verify AAE repairs missing Solr documents"),
    verify_num_match(Cluster, ?NUM_KEYS),
    %% Multiply by 3 because of N value
    ExpectedNumRepairs = length(DelKeys) * 3,
    lager:info("Verify repair count = ~p", [ExpectedNumRepairs]),
    verify_repair_count(Cluster, RepairCountBefore + ExpectedNumRepairs),
    yz_rt:stop_tracing(),
    Count = yz_rt:get_call_count(Cluster, ?REPAIR_MFA),
    ?assertEqual(ExpectedNumRepairs, Count),

    _ = verify_removal_of_orphan_postings(Cluster),

    verify_no_indefinite_repair(Cluster),

    _ = verify_no_repair_for_non_indexed_data(Cluster, PBConns),
    yz_rt:close_pb_conns(PBConns),

    _ = verify_no_repair_after_restart(Cluster),

    _ = verify_exchange_after_expire(Cluster),

    pass.

%% @doc Create a tuple containing a riak object and the first
%% partition and owner for that object.
-spec create_obj_node_partition_tuple([node()], {bucket(), binary()}) ->
                                             {obj(), node(), p()}.
create_obj_node_partition_tuple(Cluster, BKey={Bucket, Key}) ->
    Obj = riak_object:new(Bucket, Key, <<"orphan index">>, "text/plain"),
    Ring = rpc:call(hd(Cluster), yz_misc, get_ring, [transformed]),
    BProps = rpc:call(hd(Cluster), riak_core_bucket, get_bucket, [?BUCKET]),
    N = riak_core_bucket:n_val(BProps),
    PL = rpc:call(hd(Cluster), yz_misc, primary_preflist, [BKey, Ring, N]),
    {P, Node} = hd(PL),
    {Obj, Node, P}.

%% @doc Create postings for the given keys without storing
%% corresponding KV objects. This is used to simulate scenario where
%% Solr has postings for objects which no longer exist in Riak. Such a
%% scenario would happen if the KV object was deleted but the
%% corresponding delete operation for Solr failed to complete.
%%
%% NOTE: This is only creating 1 replica for each posting, not
%% N. There is no reason to create N replicas for each posting to
%% verify to correctness of AAE in this scenario.
-spec create_orphan_postings([node()], [pos_integer()]) -> ok.
create_orphan_postings(Cluster, Keys) ->
    Keys2 = [{?BUCKET, ?INT_TO_BIN(K)} || K <- Keys],
    lager:info("Create orphan postings with keys ~p", [Keys]),
    ObjNodePs = [create_obj_node_partition_tuple(Cluster, Key) || Key <- Keys2],
    [ok = rpc:call(Node, yz_kv, index, [Obj, put, P])
     || {Obj, Node, P} <- ObjNodePs],
    ok.

delete_key_in_solr(Cluster, Index, Key) ->
    [begin
         lager:info("Deleting solr doc ~s/~s on node ~p", [Index, Key, Node]),
         ok = rpc:call(Node, yz_solr, delete, [Index, [{key, list_to_binary(Key)}]])
     end || Node <- Cluster].

-spec get_cluster_repair_count([node()]) -> non_neg_integer().
get_cluster_repair_count(Cluster) ->
    lists:sum([get_total_repair_count(Node) || Node <- Cluster]).

-spec get_total_repair_count(node()) -> non_neg_integer().
get_total_repair_count(Node) ->
    Dump = rpc:call(Node, riak_kv_entropy_info, dump, []),
    YZInfo = [I || {{index,{yz,_}},_}=I <- Dump],
    Sums = [Sum || {_Key,{index_info,_,{simple_stat,_,_,_,_,Sum},_,_}} <- YZInfo],
    lists:sum(Sums).

read_schema(YZBenchDir) ->
    Path = filename:join([YZBenchDir, "schemas", "fruit_schema.xml"]),
    {ok, RawSchema} = file:read_file(Path),
    RawSchema.

setup_index(Cluster, PBConn, YZBenchDir) ->
    Node = yz_rt:select_random(Cluster),
    RawSchema = read_schema(YZBenchDir),
    ok = yz_rt:store_schema(PBConn, ?INDEX, RawSchema),
    ok = yz_rt:wait_for_schema(Cluster, ?INDEX, RawSchema),
    ok = yz_rt:create_bucket_type(Node, ?BUCKET_TYPE),
    ok = yz_rt:create_index(Node, ?INDEX, ?INDEX),
    ok = yz_rt:set_index(Node, ?BUCKET, ?INDEX),
    ok = yz_rt:wait_for_index(Cluster, ?INDEX).

%% @doc Verify that expired trees do not prevent exchange from
%% occurring (like clearing trees does).
-spec verify_exchange_after_expire([node()]) -> ok.
verify_exchange_after_expire(Cluster) ->
    lager:info("Expire all trees and verify exchange still happens"),
    lager:info("Set anti_entropy_build_limit to 0 so that trees can't be built"),
    _ = [ok = rpc:call(Node, application, set_env, [riak_kv, anti_entropy_build_limit, {0, 1000}])
         || Node <- Cluster],

    lager:info("Expire all trees"),
    _ = [ok = rpc:call(Node, yz_entropy_mgr, expire_trees, [])
         || Node <- Cluster],

    %% The expire is done very cast so just give it a moment
    timer:sleep(100),

    ok = yz_rt:wait_for_full_exchange_round(Cluster, now()),

    lager:info("Set anti_entropy_build_limit to 100 so trees can build again"),
    _ = [ok = rpc:call(Node, application, set_env, [riak_kv, anti_entropy_build_limit, {100, 1000}])
         || Node <- Cluster],
    ok.

%% @doc Verify that Yokozuna deletes postings which have no
%% corresponding KV object.
-spec verify_removal_of_orphan_postings([node()]) -> ok.
verify_removal_of_orphan_postings(Cluster) ->
    Num = random:uniform(100),
    lager:info("verify removal of ~p orphan postings", [Num]),
    yz_rt:count_calls(Cluster, ?REPAIR_MFA),
    Keys = lists:seq(?NUM_KEYS + 1, ?NUM_KEYS + Num),
    ok = create_orphan_postings(Cluster, Keys),
    ok = yz_rt:wait_for_full_exchange_round(Cluster, now()),
    ok = yz_rt:stop_tracing(),
    ?assertEqual(Num, yz_rt:get_call_count(Cluster, ?REPAIR_MFA)),
    ok.

%% @doc Verify that there is no indefinite repair.  There have been
%% several bugs in the past where Yokozuna AAE would indefinitely
%% repair.
verify_no_indefinite_repair(Cluster) ->
    lager:info("Verify no indefinite repair"),
    yz_rt:count_calls(Cluster, ?REPAIR_MFA),
    TS = erlang:now(),
    yz_rt:wait_for_full_exchange_round(Cluster, TS),
    yz_rt:stop_tracing(),
    Count = yz_rt:get_call_count(Cluster, ?REPAIR_MFA),
    ?assertEqual(0, Count).

%% @doc Verify that no repair occurrs after restart. In the past there
%% have been times when Yokozuna would unnecessairly repair data after
%% a node restart.
-spec verify_no_repair_after_restart([node()]) -> ok.
verify_no_repair_after_restart(Cluster) ->
    lager:info("verify no repair after restart"),
    _ = [ok = rt:stop(N) || N <- Cluster],
    _ = [ok = rt:wait_until_unpingable(N) || N <- Cluster],

    _ = [ok = rt:start_and_wait(N) || N <- Cluster],
    ok = rt:wait_until_nodes_ready(Cluster),
    ok = rt:wait_for_cluster_service(Cluster, yokozuna),

    yz_rt:count_calls(Cluster, ?REPAIR_MFA),

    TS2 = erlang:now(),
    yz_rt:wait_for_full_exchange_round(Cluster, TS2),

    ok = verify_repair_count(Cluster, 0),

    yz_rt:stop_tracing(),
    Count = yz_rt:get_call_count(Cluster, ?REPAIR_MFA),
    ?assertEqual(0, Count),

    ok.

%% @doc Verify that KV data written to a bucket with no associated
%% index is not repaired by AAE. When Yokozuna sees an incoming KV
%% write which has no associated index it, obviously, shouldn't index
%% the data but it should update it's AAE trees so that future
%% exchanges don't think a repair is in order. The only time when this
%% shouldn't hold is when the trees are manually cleared or
%% expired. In that case repair must happen but Yokozuna is smart
%% enough to only repair its AAE tree on not actually index the
%% data. However, in this test, the trees are not cleared and no
%% repair should hapen.
-spec verify_no_repair_for_non_indexed_data([node()], [pid()]) -> ok.
verify_no_repair_for_non_indexed_data(Cluster, PBConns) ->
    lager:info("verify no repair occurred for non-indexed data"),
    Bucket = {?BUCKET_TYPE, <<"non_indexed">>},

    %% 1. write KV data to non-indexed bucket
    Conn = yz_rt:select_random(PBConns),
    lager:info("write 100 non-indexed objects to bucket ~p", [Bucket]),
    Objs = [begin
                Key = ?INT_TO_BIN(K),
                riakc_obj:new(Bucket, Key, <<"non-indexed data">>, "text/plain")
            end
            || K <- lists:seq(1, 100)],
    [ok = riakc_pb_socket:put(Conn, O) || O <- Objs],

    %% 2. setup tracing to count repair calls
    ok = yz_rt:count_calls(Cluster, ?REPAIR_MFA),

    %% 3. wait for full exchange round
    ok = yz_rt:wait_for_full_exchange_round(Cluster, now()),
    ok = yz_rt:stop_tracing(),

    %% 4. verify repair count is 0
    ?assertEqual(0, yz_rt:get_call_count(Cluster, ?REPAIR_MFA)),
    ok.

verify_num_match(Cluster, Num) ->
    F = fun(Node) ->
                HP = hd(yz_rt:host_entries(rt:connection_info([Node]))),
                yz_rt:search_expect(HP, ?INDEX, "text", "apricot", Num)
        end,
    yz_rt:wait_until(Cluster, F).

%% @doc Verify the repair count stored in the AAE info server matches
%% what is expected.
-spec verify_repair_count([node()], non_neg_integer()) -> ok.
verify_repair_count(Cluster, ExpectedNumRepairs) ->
    RepairCount = get_cluster_repair_count(Cluster),
    ?assertEqual(ExpectedNumRepairs, RepairCount),
    ok.
