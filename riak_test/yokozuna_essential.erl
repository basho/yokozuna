-module(yokozuna_essential).
-compile(export_all).
-import(yz_rt, [host_entries/1,
                run_bb/2,
                search_expect/5, search_expect/6,
                verify_count/2,
                wait_for_joins/1, write_terms/2]).
-include_lib("eunit/include/eunit.hrl").
-include("yokozuna.hrl").

%% @doc Test essential behavior of Yokozuna.
-define(FRUIT_SCHEMA_NAME, <<"fruit">>).
-define(BUCKET_TYPE, <<"data">>).
-define(INDEX, <<"fruit_index">>).
-define(INDEX_N_VAL, 4).
-define(BUCKET, {?BUCKET_TYPE, <<"fruit">>}).
-define(NUM_KEYS, 10000).
-define(SUCCESS, 0).
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
         {yokozuna,
          [
          {enabled, true},

           %% Perform a full check every second so that non-owned
           %% postings are deleted promptly. This makes sure that
           %% postings are removed concurrent to async query during
           %% join.
           {events_full_check_after, 2}
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
            %% wait for soft-commit
            timer:sleep(1000),
            Ref = async_query(Cluster, YZBenchDir),
            %% Verify data exists before running join
            timer:sleep(30000),
            Cluster2 = join_rest(Cluster, Nodes),
            rt:wait_for_cluster_service(Cluster2, yokozuna),
            check_status(wait_for(Ref)),
            verify_non_owned_data_deleted(Cluster, ?INDEX),
            ok = test_tagging(Cluster),
            KeysDeleted = delete_some_data(Cluster2, reap_sleep()),
            verify_deletes(Cluster2, KeysDeleted, YZBenchDir),
            ok = test_escaped_key(Cluster2),
            verify_unique_id(Cluster2, PBConns),
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
    Node = yz_rt:select_random(Cluster),
    yz_rt:set_bucket_type_index(Node, T1, Index),
    yz_rt:set_bucket_type_index(Node, T2, Index),
    yz_rt:set_bucket_type_index(Node, T3, Index),
    yz_rt:set_bucket_type_index(Node, T4, Index),
    rt:wait_until_bucket_type_status(T1, active, Cluster),
    rt:wait_until_bucket_type_status(T2, active, Cluster),
    rt:wait_until_bucket_type_status(T3, active, Cluster),
    rt:wait_until_bucket_type_status(T4, active, Cluster),

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

test_tagging(Cluster) ->
    lager:info("Test tagging"),
    HP = hd(host_entries(rt:connection_info(Cluster))),
    ok = write_with_tag(HP),
    %% TODO: the test fails if this sleep isn't here
    timer:sleep(5000),
    true = search_expect(HP, <<"tagging">>, "user_s", "rzezeski", 1),
    true = search_expect(HP, <<"tagging">>, "desc_t", "description", 1),
    ok.

write_with_tag({Host, Port}) ->
    lager:info("Tag the object tagging/test"),
    URL = lists:flatten(io_lib:format("http://~s:~s/types/data/buckets/tagging/keys/test",
                                      [Host, integer_to_list(Port)])),
    Opts = [],
    Body = <<"testing tagging">>,
    Headers = [{"content-type", "text/plain"},
               {"x-riak-meta-yz-tags", "x-riak-meta-user_s, x-riak-meta-desc_t"},
               {"x-riak-meta-user_s", "rzezeski"},
               {"x-riak-meta-desc_t", "This is a description"}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body, Opts),
    ok.

async_query(Cluster, YZBenchDir) ->
    lager:info("Run async query against cluster ~p", [Cluster]),
    Hosts = host_entries(rt:connection_info(Cluster)),
    Concurrent = length(Hosts),
    Operations = [{{random_fruit_search, <<"_yz_id">>, 3, ?NUM_KEYS}, 1}],
    Cfg = [{mode, {rate,8}},
           {duration, 2},
           {concurrent, Concurrent},
           {code_paths, [YZBenchDir]},
           {driver, yz_driver},
           {operations, Operations},
           {http_conns, Hosts},
           {pb_conns, []},
           {bucket, ?BUCKET},
           {index, ?INDEX},
           {shutdown_on_error, true}],
    File = "bb-query-fruit",
    write_terms(File, Cfg),
    run_bb(async, File).

check_status({Status,_}) ->
    ?assertEqual(?SUCCESS, Status).

delete_key(Cluster, Key) ->
    Node = yz_rt:select_random(Cluster),
    lager:info("Deleting key ~s", [Key]),
    {ok, C} = riak:client_connect(Node),
    C:delete(?BUCKET, Key).

delete_some_data(Cluster, ReapSleep) ->
    Keys = yz_rt:random_keys(?NUM_KEYS),
    lager:info("Deleting ~p keys", [length(Keys)]),
    [delete_key(Cluster, K) || K <- Keys],
    lager:info("Sleeping ~ps to allow for reap", [ReapSleep]),
    timer:sleep(timer:seconds(ReapSleep)),
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

reap_sleep() ->
    %% NOTE: This is hardcoded to 5s now but if this test ever allows
    %%       configuation of deletion policy then this should be
    %%       calculated.
    10.

setup_indexing(Cluster, PBConns, YZBenchDir) ->
    Node = yz_rt:select_random(Cluster),
    PBConn = yz_rt:select_random(PBConns),

    yz_rt:create_bucket_type(Node, ?BUCKET_TYPE),

    RawSchema = read_schema(YZBenchDir),
    yz_rt:store_schema(PBConn, ?FRUIT_SCHEMA_NAME, RawSchema),
    yz_rt:wait_for_schema(Cluster, ?FRUIT_SCHEMA_NAME, RawSchema),
    ok = yz_rt:create_index(Node, ?INDEX, ?FRUIT_SCHEMA_NAME, ?INDEX_N_VAL),
    ok = yz_rt:create_index(Node, <<"tagging">>),
    ok = yz_rt:create_index(Node, <<"escaped">>),
    ok = yz_rt:create_index(Node, <<"unique">>),

    [yz_rt:wait_for_index(Cluster, I)
     || I <- [?INDEX, <<"tagging">>, <<"escaped">>, <<"unique">>]],

    yz_rt:set_index(Node, ?BUCKET, ?INDEX, ?INDEX_N_VAL),
    yz_rt:set_index(Node, {?BUCKET_TYPE, <<"tagging">>}, <<"tagging">>),
    yz_rt:set_index(Node, {?BUCKET_TYPE, <<"escaped">>}, <<"escaped">>).

verify_deletes(Cluster, KeysDeleted, YZBenchDir) ->
    NumDeleted = length(KeysDeleted),
    lager:info("Verify ~p keys were deleted", [NumDeleted]),
    Hosts = host_entries(rt:connection_info(Cluster)),
    Concurrent = length(Hosts),
    Apple = {search,"apple","id",?NUM_KEYS - NumDeleted},

    Cfg = [{mode, max},
           {duration, 1},
           {concurrent, Concurrent},
           {driver, yz_driver},
           {code_paths, [YZBenchDir]},
           {operations, [{Apple,1}]},
           {http_conns, Hosts},
           {pb_conns, []},
           {bucket, ?BUCKET},
           {index, ?INDEX},
           {shutdown_on_error, true}],
    File = "bb-verify-deletes",
    write_terms(File, Cfg),
    check_status(run_bb(sync, File)).

wait_for(Ref) ->
    rt:wait_for_cmd(Ref).
