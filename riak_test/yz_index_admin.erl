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
    pass.

%% @doc Test basic creation, no body.
confirm_create_index_1(Cluster) ->
    Node = select_random(Cluster),
    Index = "test_index_1",
    lager:info("confirm_create_index_1 ~s [~p]", [Index, Node]),
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    URL = index_url(HP, Index),
    {ok, Status, _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("204", Status).

%% @doc Test index creation when passing schema name.
confirm_create_index_2(Cluster) ->
    Node = select_random(Cluster),
    Index = "test_index_2",
    lager:info("confirm_create_index_2 ~s [~p]", [Index, Node]),
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    URL = index_url(HP, Index),
    Headers = [{"content-type", "application/json"}],
    Body = <<"{\"schema\":\"_yz_default\"}">>,
    {ok, Status, _, _} = http(put, URL, Headers, Body),
    ?assertEqual("204", Status).

confirm_409(Cluster) ->
    Node = select_random(Cluster),
    Index = "test_index_409",
    lager:info("confirm_409 ~s [~p]", [Index, Node]),
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    URL = index_url(HP, Index),
    {ok, Status1, _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("204", Status1),
    %% Currently need to sleep to wait for Solr to create index
    timer:sleep(5000),
    {ok, Status2, _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("409", Status2).

confirm_list(Cluster, Indexes) ->
    Node = select_random(Cluster),
    lager:info("confirm_list ~p [~p]", [Indexes, Node]),
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    URL = index_list_url(HP),
    {ok, Status, _, Body} = http(get, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("200", Status),
    ?assert(check_list(Indexes, Body)).

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
    Cluster.

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

wait_for_joins(Cluster) ->
    lager:info("Waiting for ownership handoff to finish"),
    rt:wait_until_nodes_ready(Cluster),
    rt:wait_until_no_pending_changes(Cluster).
