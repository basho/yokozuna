%% @doc Test the index adminstration API in various ways.
-module(yz_index_admin).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(NO_HEADERS, []).
-define(NO_BODY, <<>>).
confirm() ->
    Cluster = prepare_cluster(4),
    confirm_create_index_1(Cluster),
    confirm_create_index_2(Cluster),
    confirm_409(Cluster),
    confirm_list(Cluster, ["test_index_1", "test_index_2", "test_index_409"]),
    confirm_delete(Cluster, "test_index_1"),
    confirm_get(Cluster, "test_index_2"),
    confirm_404(Cluster, "not_an_index"),
    pass.

%% @doc Test basic creation, no body.
confirm_create_index_1(Cluster) ->
    Index = "test_index_1",
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_create_index_1 ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    {ok, Status, _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("204", Status).

%% @doc Test index creation when passing schema name.
confirm_create_index_2(Cluster) ->
    Index = "test_index_2",
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_create_index_2 ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    Headers = [{"content-type", "application/json"}],
    Body = <<"{\"schema\":\"_yz_default\"}">>,
    {ok, Status, _, _} = http(put, URL, Headers, Body),
    ?assertEqual("204", Status).

confirm_409(Cluster) ->
    Index = "test_index_409",
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_409 ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    {ok, Status1, _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("204", Status1),
    %% Currently need to sleep to wait for Solr to create index
    timer:sleep(5000),
    {ok, Status2, _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("409", Status2).

confirm_list(Cluster, Indexes) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_list ~p [~p]", [Indexes, HP]),
    URL = index_list_url(HP),
    {ok, Status, _, Body} = http(get, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("200", Status),
    ?assert(check_list(Indexes, Body)).

confirm_delete(Cluster, Index) ->
    confirm_get(Cluster, Index),
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_delete ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    %% Don't use confirm_get of confirm_404 here as want to hit the
    %% same node, otherwise have to sleep for ring propagation.
    %% Should look into storing the index list in Riak now that PR/PW
    %% has been fixed.
    {ok, Status1, _, _} = http(get, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("200", Status1),
    {ok, Status2, _, _} = http(delete, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("204", Status2),
    {ok, Status3, _, _} = http(get, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("404", Status3).

confirm_get(Cluster, Index) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_get ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    {ok, Status, Headers, _} = http(get, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("200", Status),
    ?assertEqual("application/json", ct(Headers)).

confirm_404(Cluster, Index) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_404 ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    {ok, Status, _, _} = http(get, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("404", Status).

%%%===================================================================
%%% Helpers
%%%===================================================================

check_list(Indexes, Body) ->
    Decoded = mochijson2:decode(Body),
    Names = [binary_to_list(proplists:get_value(<<"name">>, Obj))
             || {struct, Obj} <- Decoded],
    lists:sort(Indexes) == lists:sort(Names).

contains_index(_Body) ->
    undefined.

ct(Headers) ->
    Headers2 = [{string:to_lower(Key), Value} || {Key, Value} <- Headers],
    proplists:get_value("content-type", Headers2).

host_entries(ClusterConnInfo) ->
    [proplists:get_value(http, I) || {_,I} <- ClusterConnInfo].

http(Method, URL, Headers, Body) ->
    Opts = [],
    ibrowse:send_req(URL, Headers, Method, Body, Opts).

index_list_url({Host, Port}) ->
    ?FMT("http://~s:~B/yz/index", [Host, Port]).

index_url({Host,Port}, Index) ->
    ?FMT("http://~s:~B/yz/index/~s", [Host, Port, Index]).

join(Nodes) ->
    [NodeA|Others] = Nodes,
    [rt:join(Node, NodeA) || Node <- Others],
    Nodes.

prepare_cluster(NumNodes) ->
    %% Note: may need to use below call b/c of diff between
    %% deploy_nodes/1 & /2
    %%
    %% Nodes = rt:deploy_nodes(NumNodes, ?CFG),
    Nodes = rt:deploy_nodes(NumNodes),
    Cluster = join(Nodes),
    wait_for_joins(Cluster),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    Cluster.

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

wait_for_joins(Cluster) ->
    lager:info("Waiting for ownership handoff to finish"),
    rt:wait_until_nodes_ready(Cluster),
    rt:wait_until_no_pending_changes(Cluster).
