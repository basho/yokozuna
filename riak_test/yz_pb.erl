%% @doc Test the index adminstration API in various ways.
-module(yz_pb).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(NO_HEADERS, []).
-define(NO_BODY, <<>>).

confirm() ->
    YZBenchDir = rt_config:get_os_env("YZ_BENCH_DIR"),
    code:add_path(filename:join([YZBenchDir, "ebin"])),
    random:seed(now()),
    Cluster = prepare_cluster(4),
    confirm_basic_search(Cluster),
    confirm_encoded_search(Cluster),
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

%% TODO: replace with rt:wait_for_joins
wait_for_joins(Cluster) ->
    lager:info("Waiting for ownership handoff to finish"),
    rt:wait_until_nodes_ready(Cluster),
    rt:wait_until_no_pending_changes(Cluster).

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

host_entries(ClusterConnInfo) ->
    [proplists:get_value(http, I) || {_,I} <- ClusterConnInfo].

schema_url({Host,Port}, Name) ->
    ?FMT("http://~s:~B/yz/schema/~s", [Host, Port, Name]).

index_url({Host,Port}, Index) ->
    ?FMT("http://~s:~B/yz/index/~s", [Host, Port, Index]).

bucket_url({Host,Port}, Bucket, Key) ->
    ?FMT("http://~s:~B/riak/~s/~s", [Host, Port, Bucket, Key]).

http(Method, URL, Headers, Body) ->
    Opts = [],
    ibrowse:send_req(URL, Headers, Method, Body, Opts).

create_index(HP, Index) ->
    lager:info("create_index ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    Headers = [{"content-type", "application/json"}],
    {ok, Status, _, _} = http(put, URL, Headers, ?NO_BODY),
    timer:sleep(4000),
    ?assertEqual("204", Status).

store_and_search(Cluster, Bucket, Key, Body, Search, Params) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    create_index(HP, Bucket),
    URL = bucket_url(HP, Bucket, Key),
    lager:info("Storing to bucket ~s", [URL]),
    {Host, Port} = HP,
    %% populate a value
    {ok, "204", _, _} = ibrowse:send_req(URL, [{"Content-Type", "text/plain"}], put, Body),
    %% Sleep for soft commit
    timer:sleep(1100),
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    lager:info("search_results"),
    {ok,{search_results,[{Bucket,Results}],Score,Found}} =
            riakc_pb_socket:search(Pid, Bucket, Search, Params),
    ?assertEqual(
        Key,
        binary_to_list(proplists:get_value(<<"_yz_rk">>, Results))),
    ?assertEqual(Found, 1),
    ?assertNot(Score == 0.0),
    ok.

confirm_basic_search(Cluster) ->
    Bucket = "basic",
    lager:info("confirm_basic_search ~s", [Bucket]),
    Body = "herp derp",
    Params = [{sort, <<"score desc">>}, {fl, ["*","score"]}],
    store_and_search(Cluster, Bucket, "test", Body, <<"text:herp">>, Params).

confirm_encoded_search(Cluster) ->
    Bucket = "encoded",
    lager:info("confirm_encoded_search ~s", [Bucket]),
    Body = "א בְּרֵאשִׁית, בָּרָא אֱלֹהִים, אֵת הַשָּׁמַיִם, וְאֵת הָאָרֶץ",
    Params = [{sort, <<"score desc">>}, {fl, ["_yz_rk"]}],
    store_and_search(Cluster, Bucket, "וְאֵת", Body, <<"text:בָּרָא">>, Params).
