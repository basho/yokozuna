-module(yokozuna_essential).
-compile(export_all).
-import(yz_rt, [host_entries/1,
                search_expect/5, search_expect/6,
                wait_for_joins/1]).
-include_lib("eunit/include/eunit.hrl").
-include("yokozuna.hrl").

%% @doc Test essential behavior of Yokozuna.
-define(FRUIT_SCHEMA_NAME, <<"fruit">>).
-define(BUCKET_TYPE, <<"data">>).
-define(INDEX, <<"fruit_index">>).
-define(INDEX_N_VAL, 4).
-define(BUCKET, {?BUCKET_TYPE, <<"fruit">>}).
-define(NUM_KEYS, 10000).
-define(REAP_TIME_IN_MS, 10000).
-define(CFG,
        [{riak_core,
          [
           %% Allow handoff to happen more quickly.
           {handoff_concurrency, 3},

           %% Use smaller ring size so that test runs faster.
           {ring_creation_size, 16},

           %% Reduce the tick so that ownership handoff will happen
           %% more quickly.
           {vnode_management_timer, 1000}
          ]},
         {riak_kv,
          [
           %% Max number of times that a secondary system can block
           %% handoff of primary key-value data.
           %% Set to prevent a race-condition when propagating cluster
           %% meta to newly joined nodes.
           {handoff_rejected_max, infinity},
           %% Set delete_mode on a chosen time.
           {riak_kv, [{delete_mode, ?REAP_TIME_IN_MS}]}
          ]},
         {yokozuna,
          [
          {enabled, true},

           %% Perform a full check every second so that non-owned
           %% postings are deleted promptly.
           {events_full_check_after, 2},
           %% Adjust batching to force flushing ASAP
           {solrq_batch_max, 1000000},
           {solrq_delayms_max, 1}
          ]}
        ]).

confirm() ->
    case yz_rt:bb_driver_setup() of
        {ok, YZBenchDir} ->
            random:seed(now()),
            Nodes = rt:deploy_nodes(4, ?CFG),
            Cluster = join(Nodes, 2),
            PBConns = yz_rt:open_pb_conns(Cluster),
            wait_for_joins(Cluster),
            rt:wait_for_cluster_service(Cluster, yokozuna),
            setup_indexing(Cluster, PBConns, YZBenchDir),
            verify_non_existent_index(Cluster, <<"froot">>),
            {0, _} = yz_rt:load_data(Cluster, ?BUCKET, YZBenchDir, ?NUM_KEYS),
            yz_rt:commit(Cluster, ?INDEX),
            verify_correct_solrqs(Cluster),
            %% Verify data exists before running join
            yz_rt:verify_num_match(Cluster, ?INDEX, ?NUM_KEYS),
            Cluster2 = join_rest(Cluster, Nodes),
            rt:wait_for_cluster_service(Cluster2, riak_kv),
            rt:wait_for_cluster_service(Cluster2, yokozuna),
            verify_non_owned_data_deleted(Cluster, ?INDEX),
            verify_correct_solrqs(Cluster2),
            wait_for_indexes(Cluster2),
            ok = test_tagging_http(Cluster2),
            ok = test_tagging_pb(Cluster2),
            KeysDeleted = delete_some_data(Cluster2, ?BUCKET, ?NUM_KEYS,
                                           ?REAP_TIME_IN_MS),
            test_really_deleted(Cluster2, ?BUCKET, KeysDeleted),
            yz_rt:commit(Cluster2, ?INDEX),
            verify_deletes(Cluster2, ?INDEX, ?NUM_KEYS, KeysDeleted),
            ok = test_escaped_key(Cluster2),
            verify_unique_id(Cluster2, PBConns),
            verify_deleted_index_stops_solrqs(Cluster2, PBConns),
            yz_rt:close_pb_conns(PBConns),
            pass;
        {error, bb_driver_build_failed} ->
            lager:info("Failed to build the yokozuna basho_bench driver"
                       " required for this test"),
            fail
    end.

%% @doc Verify that indexes for non-owned data have been
%% deleted. I.e. after a node performs ownership handoff to a joining
%% node it should delete indexes for the partitions it no longer owns.
verify_non_owned_data_deleted(Cluster, Index) ->
    yz_rt:wait_until(Cluster, is_non_owned_data_deleted(Index)).

%% @doc Verify that a non-existent index returns a 404 error.
verify_non_existent_index(Cluster, BadIndex) ->
    HP = hd(host_entries(rt:connection_info(Cluster))),
    {ok, "404", _, _} = yz_rt:search(HP, BadIndex, <<"field">>, <<"term">>).

%% @doc Predicate to determine if the node's indexes for non-owned
%% data have been deleted.
is_non_owned_data_deleted(Index) ->
    fun(Node) ->
            Ring = rpc:call(Node, yz_misc, get_ring, [raw]),
            PartitionList = rpc:call(Node, yokozuna, partition_list, [Index]),
            IndexPartitions = rpc:call(Node, yz_cover, reify_partitions, [Ring, PartitionList]),
            OwnedAndNext = rpc:call(Node, yz_misc, owned_and_next_partitions, [Node, Ring]),
            NonOwned = ordsets:subtract(IndexPartitions, OwnedAndNext),
            LNonOwned = rpc:call(Node, yz_cover, logical_partitions, [Ring, NonOwned]),
            QueryStrings = [?YZ_PN_FIELD_S ++ ":" ++ integer_to_list(LP)
                            || LP <- LNonOwned],
            Query = list_to_binary(string:join(QueryStrings, " OR ")),
            {_, Resp} = rpc:call(Node, yz_solr, search, [Index, [], [{q, Query}, {wt, <<"json">>}]]),
            Decoded = mochijson2:decode(Resp),
            NumFound = kvc:path([<<"response">>, <<"numFound">>], Decoded),
            lager:info("checking node ~p for non-owned data in index ~p, E: 0 A: ~p",
                       [Node, Index, NumFound]),
            0 == NumFound
    end.

%% @doc Verify that Solr documents are unique based on type + bucket +
%% key + partition.
-define(RC(PCConns), yz_rt:select_random(PBConns)).
verify_unique_id(Cluster, PBConns) ->
    Index = <<"unique">>,
    T1 = <<"t1/λ/{1}+-&&||!()[]^\"~*?:\\">>,
    T2 = <<"t2/λ/{1}+-&&||!()[]^\"~*?:\\">>,
    T3 = <<"btx">>,
    T4 = <<"btx*bky">>,
    B1 = {T1, <<"b1/λ/{1}+-&&||!()[]^\"~*?:\\">>},
    B2 = {T1, <<"b2/λ/{1}+-&&||!()[]^\"~*?:\\">>},
    B3 = {T2, <<"b1/λ/{1}+-&&||!()[]^\"~*?:\\">>},
    B4 = {T2, <<"b2/λ/{1}+-&&||!()[]^\"~*?:\\">>},
    B5 = {T2, <<"b3/λ/{1}+-&&||!()[]^\"~*?:\\">>},
    B6 = {T3, <<"bky*bt">>},
    B7 = {T4, <<"bt">>},
    Key = <<"key">>,
    Val = <<"yokozuna">>,
    CT = "text/plain",
    Query = <<"text:yokozuna">>,

    %% These are objects with distinct buckets but all share the same
    %% key.  The point is to make sure that uniqueness is defined by
    %% the combination of bucket (including the type) and key.
    %%
    %% The O4 & O5 objects contain siblings.  These are needed to
    %% exercise the logic for deletion when an objects siblings are
    %% resolved (`yz_kv:cleanup').
    O1 = riakc_obj:new(B1, Key, Val, CT),
    O2 = riakc_obj:new(B2, Key, Val, CT),
    O3 = riakc_obj:new(B3, Key, Val, CT),
    O4 = riakc_obj:new(B4, Key, Val, CT),
    O4Sib = riakc_obj:new(B4, Key, Val, CT),
    O5 = riakc_obj:new(B5, Key, Val, CT),
    O5Sib = riakc_obj:new(B5, Key, Val, CT),
    O6 = riakc_obj:new(B6, Key, Val, CT),
    O7 = riakc_obj:new(B7, Key, Val, CT),

    %% Associate index with bucket-types
    yz_rt:set_bucket_type_index(Cluster, T1, Index),
    yz_rt:set_bucket_type_index(Cluster, T2, Index),
    yz_rt:set_bucket_type_index(Cluster, T3, Index),
    yz_rt:set_bucket_type_index(Cluster, T4, Index),

    lager:info("Write 4 objects, verify query result"),
    {ok, O1R} = riakc_pb_socket:put(?RC(PBConns), O1, [return_body]),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 1))),

    {ok, O2R} = riakc_pb_socket:put(?RC(PBConns), O2, [return_body]),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 2))),

    {ok, O3R} = riakc_pb_socket:put(?RC(PBConns), O3, [return_body]),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 3))),

    {ok, _O4R} = riakc_pb_socket:put(?RC(PBConns), O4, [return_body]),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 4))),

    lager:info("Create a sibling in object 4, verify query result"),
    {ok, O4SibR} = riakc_pb_socket:put(?RC(PBConns), O4Sib, [return_body]),
    2 = riakc_obj:value_count(O4SibR),
    O4Sib2 = riakc_obj:select_sibling(1, O4SibR),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 5))),

    lager:info("Create object 5, verify object 4 sibling indexes not deleted"),
    {ok, _O5R} = riakc_pb_socket:put(?RC(PBConns), O5, [return_body]),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 6))),

    lager:info("Create object 5 sibling, verify query result"),
    {ok, O5SibR} = riakc_pb_socket:put(?RC(PBConns), O5Sib, [return_body]),
    2 = riakc_obj:value_count(O5SibR),
    O5Sib2 = riakc_obj:select_sibling(1, O5SibR),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 7))),

    lager:info("Resolve object 5 siblings, verify object 4 sibling "
               "indexes not deleted"),
    {ok, O5SibR2} = riakc_pb_socket:put(?RC(PBConns), O5Sib2, [return_body]),
    1 = riakc_obj:value_count(O5SibR2),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 6))),

    lager:info("Resolve object 4 siblings, verify query result"),
    {ok, O4SibR2} = riakc_pb_socket:put(?RC(PBConns), O4Sib2, [return_body]),
    1 = riakc_obj:value_count(O4SibR2),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 5))),

    lager:info("Creating two potential overlapping type/bucket names"),
    {ok, O6R} = riakc_pb_socket:put(?RC(PBConns), O6, [return_body]),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 6))),

    {ok, O7R} = riakc_pb_socket:put(?RC(PBConns), O7, [return_body]),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 7))),


    lager:info("Delete all objs, verify only respective indexes are deleted"),
    ok = riakc_pb_socket:delete_obj(?RC(PBConns), O7R),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 6))),

    ok = riakc_pb_socket:delete_obj(?RC(PBConns), O6R),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 5))),

    ok = riakc_pb_socket:delete_obj(?RC(PBConns), O5SibR2),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 4))),

    ok = riakc_pb_socket:delete_obj(?RC(PBConns), O4SibR2),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 3))),

    ok = riakc_pb_socket:delete_obj(?RC(PBConns), O3R),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 2))),

    ok = riakc_pb_socket:delete_obj(?RC(PBConns), O2R),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 1))),

    ok = riakc_pb_socket:delete_obj(?RC(PBConns), O1R),
    ?assertEqual(ok, rt:wait_until(make_query_fun(PBConns, Index, Query, 0))).

make_query_fun(PBConns, Index, Query, Expected) ->
    fun() ->
            QueryRes = query_all(PBConns, Index, Query),
            lists:all(fun(X) ->
                lager:info("~p~n", [X]),
                X == Expected end, QueryRes)
    end.

query_all(PBConns, Index, Query) ->
    [begin
         Conn,
         {ok,{_, _, _, Found}} = riakc_pb_socket:search(?RC(Conn), Index, Query, []),
         Found
     end || Conn <- PBConns].

%% @doc Verify that the delete call made my `yz_kv:cleanup' can deal
%% with a key which contains characters like '/', ':' and '-'.
test_escaped_key(Cluster) ->
    lager:info("Test key escape"),
    {H, P} = hd(host_entries(rt:connection_info(Cluster))),
    Value = <<"Never gonna give you up">>,
    Bucket = {<<"data">>,<<"escaped">>},
    Key = edoc_lib:escape_uri("rick/astley-rolled:derp"),
    ok = yz_rt:http_put({H, P}, Bucket, Key, Value),
    ok = http_get({H, P}, Bucket, Key),
    ok.

http_get({Host, Port}, {BType, BName}, Key) ->
    URL = lists:flatten(io_lib:format("http://~s:~s/types/~s/buckets/~s/keys/~s",
                                      [Host, integer_to_list(Port), BType, BName, Key])),
    Headers = [{"accept", "text/plain"}],
    {ok, "200", _, _} = ibrowse:send_req(URL, Headers, get, [], []),
    ok.

test_really_deleted(Cluster, Bucket, DelKeys) ->
    lager:info("Test if keys really deleted from riak"),
    ANode = yz_rt:select_random(Cluster),
    rt:pbc_really_deleted(rt:pbc(ANode), Bucket, DelKeys).

test_tagging_http(Cluster) ->
    lager:info("Test tagging http"),
    HP = hd(host_entries(rt:connection_info(Cluster))),
    ANode = yz_rt:select_random(Cluster),
    ok = write_with_tag_http(HP),
    Index = <<"tagging_http">>,
    yz_rt:commit(Cluster, Index),
    ok = search_expect(ANode, Index, "user_s", "rzezeski", 1),
    ok = search_expect(ANode, Index, "desc_t", "description", 1).

test_tagging_pb(Cluster) ->
    lager:info("Test tagging pb"),
    HP = hd(host_entries(rt:connection_info(Cluster))),
    ANode = yz_rt:select_random(Cluster),
    ok = write_with_tag_pb(HP),
    Index = <<"tagging_pb">>,
    yz_rt:commit(Cluster, Index),
    ok = search_expect(ANode, Index, "user_s", "rzezeski", 1),
    ok = search_expect(ANode, Index, "desc_t", "description", 1).

write_with_tag_http({Host, Port}) ->
    lager:info("Tag the object tagging/test (http)"),
    URL = lists:flatten(io_lib:format("http://~s:~s/types/data/buckets/tagging_http/keys/test",
                                      [Host, integer_to_list(Port)])),
    Opts = [],
    Body = <<"testing tagging">>,
    Headers = [{"content-type", "text/plain"},
               {"x-riak-meta-yz-tags", "x-riak-meta-user_s, x-riak-meta-desc_t"},
               {"x-riak-meta-user_s", "rzezeski"},
               {"x-riak-meta-desc_t", "This is a description"}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body, Opts),
    ok.

%% @doc tagging via the pb_client should not need `x-riak-meta*'
%%      (used for http-headers), but this is the current workaround until
%%      we make the pb interface better.
write_with_tag_pb({Host, Port}) ->
    lager:info("Tag the object tagging/test (pb)"),
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),

    O0 = riakc_obj:new({<<"data">>, <<"tagging_pb">>}, <<"test">>, <<"testing tagging">>),
    MD0 = riakc_obj:get_update_metadata(O0),
    MD1 = riakc_obj:set_user_metadata_entry(MD0, {<<"x-riak-meta-yz-tags">>,
                                                  <<"x-riak-meta-user_s, x-riak-meta-desc_t">>}),
    MD2 = riakc_obj:set_user_metadata_entry(MD1, {<<"x-riak-meta-user_s">>,<<"rzezeski">>}),
    MD3 = riakc_obj:set_user_metadata_entry(MD2, {<<"x-riak-meta-desc_t">>,<<"This is a description">>}),
    O1 = riakc_obj:update_metadata(O0, MD3),
    O2 = riakc_obj:update_content_type(O1, "text/plain"),
    ok = riakc_pb_socket:put(Pid, O2),
    riakc_pb_socket:stop(Pid),
    ok.

dump_bb_logs() ->
    Logs = filelib:wildcard("/tmp/yz-bb-results/current/*.log"),
    lists:foreach(fun dump_log/1, Logs).

dump_log(Log) ->
    lager:info("--- Dumping log file ~p ---", [Log]),
    {ok, File} = file:open(Log, [read]),
    dump_file(File),
    lager:info("--- End log file dump of ~p ---", [Log]).

dump_file(File) ->
    case file:read_line(File) of
        eof ->
            ok;
        {ok, Data} ->
            lager:info("~s", [Data]),
            dump_file(File)
    end.

delete_key(Cluster, Bucket, Key) ->
    Node = yz_rt:select_random(Cluster),
    lager:info("Deleting key ~s", [Key]),
    {ok, C} = riak:client_connect(Node),
    ok = C:delete(Bucket, Key).

delete_some_data(Cluster, Bucket, NumKeys, ReapSleep) ->
    Keys = lists:usort(yz_rt:random_keys(NumKeys)),
    lager:info("Deleting ~p keys", [length(Keys)]),
    [ok = delete_key(Cluster, Bucket, K) || K <- Keys],
    lager:info("Sleeping ~ps to allow for reap", [ReapSleep/1000]),
    timer:sleep(ReapSleep),
    Keys.

-spec join([node()], pos_integer()) -> [node()].
join(Nodes, Num) ->
    [NodeA|Others] = All = lists:sublist(Nodes, Num),
    [rt:join(Node, NodeA) || Node <- Others],
    All.

join_rest([NodeA|_]=Cluster, Nodes) ->
    ToJoin = Nodes -- Cluster,
    [begin rt:join(Node, NodeA) end || Node <- ToJoin],
    Nodes.

read_schema(YZBenchDir) ->
    Path = filename:join([YZBenchDir, "schemas", "fruit_schema.xml"]),
    {ok, RawSchema} = file:read_file(Path),
    RawSchema.

setup_indexing(Cluster, PBConns, YZBenchDir) ->
    Node = yz_rt:select_random(Cluster),
    PBConn = yz_rt:select_random(PBConns),

    yz_rt:create_bucket_type(Cluster, ?BUCKET_TYPE),

    RawSchema = read_schema(YZBenchDir),
    yz_rt:store_schema(PBConn, ?FRUIT_SCHEMA_NAME, RawSchema),
    yz_rt:wait_for_schema(Cluster, ?FRUIT_SCHEMA_NAME, RawSchema),
    ok = yz_rt:create_index(Cluster, ?INDEX, ?FRUIT_SCHEMA_NAME, ?INDEX_N_VAL),
    ok = yz_rt:create_index(Cluster, <<"tagging_http">>),
    ok = yz_rt:create_index(Cluster, <<"tagging_pb">>),
    ok = yz_rt:create_index(Cluster, <<"escaped">>),
    ok = yz_rt:create_index(Cluster, <<"unique">>),

    yz_rt:set_index(Node, ?BUCKET, ?INDEX, ?INDEX_N_VAL),
    yz_rt:set_index(Node, {?BUCKET_TYPE, <<"tagging_http">>}, <<"tagging_http">>),
    yz_rt:set_index(Node, {?BUCKET_TYPE, <<"tagging_pb">>}, <<"tagging_pb">>),
    yz_rt:set_index(Node, {?BUCKET_TYPE, <<"escaped">>}, <<"escaped">>).

wait_for_indexes(Cluster) ->
    ok = yz_rt:wait_for_index(Cluster, ?INDEX),
    ok = yz_rt:wait_for_index(Cluster, <<"tagging_http">>),
    ok = yz_rt:wait_for_index(Cluster, <<"tagging_pb">>),
    ok = yz_rt:wait_for_index(Cluster, <<"escaped">>),
    ok = yz_rt:wait_for_index(Cluster, <<"unique">>).

verify_deletes(Cluster, Index, NumKeys, KeysDeleted) ->
    NumDeleted = length(KeysDeleted),
    lager:info("Verify ~p keys were deleted", [NumDeleted]),
    yz_rt:verify_num_match(Cluster, Index, NumKeys - NumDeleted).


verify_deleted_index_stops_solrqs(Cluster, PBConns) ->
    PBConn = hd(PBConns),
    yz_rt:really_remove_index(Cluster, ?BUCKET, ?INDEX, PBConn),
    verify_correct_solrqs(Cluster).

verify_correct_solrqs(Cluster) ->
    ?assertEqual(ok, rt:wait_until(Cluster, fun check_queues_match/1)).

check_queues_match(Node) ->
    %% Current Indexes includes ?YZ_INDEX_TOMBSTONE because we need to write the entries
    %% for non-indexed data to the YZ AAE tree. Excluding them makes the solrq supervisor
    %% constantly start and stop these queues.
    CurrentIndexes = rpc:call(Node, yz_index, get_indexes_from_meta, []) ++ [?YZ_INDEX_TOMBSTONE],
    OwnedPartitions = rt:partitions_for_node(Node),
    ActiveQueues = rpc:call(Node, yz_solrq_sup, active_queues, []),
    ExpectedQueueus = [{Index, Partition} || Index <- CurrentIndexes, Partition <- OwnedPartitions],
    lager:debug("Validating correct Solr Queues are running. Node: ~p, Expected: ~p, Active:  ~p", [Node, ExpectedQueueus, ActiveQueues]),
    lists:sort(ExpectedQueueus) == lists:sort(ActiveQueues).

