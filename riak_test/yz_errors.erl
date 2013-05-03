%% @doc Ensure that error messages return as expected
-module(yz_errors).
-compile(export_all).
-import(yz_rt, [host_entries/1,
                run_bb/2, search_expect/5,
                set_index_flag/2,
                select_random/1, verify_count/2,
                wait_for_joins/1, write_terms/2]).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(NO_HEADERS, []).
-define(NO_BODY, <<>>).

confirm() ->
    YZBenchDir = rt_config:get_os_env("YZ_BENCH_DIR"),
    code:add_path(filename:join([YZBenchDir, "ebin"])),
    random:seed(now()),
    Cluster = prepare_cluster(4),
    ok = test_errors(Cluster),
    pass.

prepare_cluster(NumNodes) ->
    %% Note: may need to use below call b/c of diff between
    %% deploy_nodes/1 & /2
    %%
    % Nodes = rt:deploy_nodes(NumNodes, ?CFG),
    Nodes = rt:deploy_nodes(NumNodes),
    Cluster = join(Nodes),
    wait_for_joins(Cluster),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    Cluster.

join(Nodes) ->
    [NodeA|Others] = Nodes,
    [rt:join(Node, NodeA) || Node <- Others],
    Nodes.

test_errors(Cluster) ->
    HP = hd(host_entries(rt:connection_info(Cluster))),
    ok = expect_bad_json(HP),
    ok = expect_bad_xml(HP),
    ok = expect_bad_query(HP),
    ok.

expect_bad_json(HP) ->
    ok = create_index(HP, "bad_json"),
    lager:info("Write bad json"),
    URL = bucket_url(HP, "bad_json", "test"),
    Opts = [],
    CT = "application/json",
    Headers = [{"content-type", CT}],
    Body = "{\"bad\": \"unclosed\"",
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body, Opts),
    %% Sleep for soft commit
    timer:sleep(1100),
    %% still store the value in riak
    {ok, "200", _, Body} = ibrowse:send_req(URL, [{"accept", CT}], get, []),
    ?assert(search_expect(HP, "bad_json", ?YZ_ERR_FIELD_S, "1", 1)),
    ok.

expect_bad_xml(HP) ->
    ok = create_index(HP, "bad_xml"),
    lager:info("Write bad xml"),
    URL = bucket_url(HP, "bad_xml", "test"),
    Opts = [],
    CT = "application/xml",
    Headers = [{"content-type", CT}],
    Body = "<\"bad\" \"xml\"></",
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body, Opts),
    %% Sleep for soft commit
    timer:sleep(1100),
    %% still store the value in riak
    {ok, "200", _, Body} = ibrowse:send_req(URL, [{"accept", CT}], get, []),
    ?assert(search_expect(HP, "bad_xml", ?YZ_ERR_FIELD_S, "1", 1)),
    ok.

expect_bad_query(HP) ->
    ok = create_index(HP, "bad_query"),
    lager:info("Write bad query"),
    URL = bucket_url(HP, "bad_query", "test"),
    Opts = [],
    CT = "text/plain",
    Headers = [{"content-type", CT}],
    Body = "",
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body, Opts),
    %% Sleep for soft commit
    timer:sleep(1100),
    %% still store the value in riak
    {ok, "200", _, Body} = ibrowse:send_req(URL, [{"accept", CT}], get, []),
    %% send a bad query
    SearchURL = search_url(HP, "bad_query") ++ "?q=*:*&sort=sco+desc",
    {ok, "400", _, _} = ibrowse:send_req(SearchURL, [], get, []),
    ok.

index_url({Host,Port}, Index) ->
    ?FMT("http://~s:~B/yz/index/~s", [Host, Port, Index]).

bucket_url({Host,Port}, Bucket, Key) ->
    ?FMT("http://~s:~B/riak/~s/~s", [Host, Port, Bucket, Key]).

search_url({Host,Port}, Bucket) ->
    ?FMT("http://~s:~B/search/~s", [Host, Port, Bucket]).

http(Method, URL, Headers, Body) ->
    Opts = [],
    ibrowse:send_req(URL, Headers, Method, Body, Opts).

create_index(HP, Index) ->
    lager:info("create_index ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    Headers = [{"content-type", "application/json"}],
    {ok, Status, _, _} = http(put, URL, Headers, ?NO_BODY),
    timer:sleep(5000),
    ?assertEqual("204", Status).
