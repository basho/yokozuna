%% @doc Test the index adminstration API in various ways.
-module(yz_index_admin).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(NO_HEADERS, []).
-define(NO_BODY, <<>>).
-define(CFG,
        [
         {riak_core,
          [
           {ring_creation_size, 8}
          ]},
         {yokozuna,
          [
           {enabled, true}
          ]}
        ]).


confirm() ->
    Cluster = prepare_cluster(4),
    confirm_create_index_1(Cluster),
    confirm_create_index_2(Cluster),
    confirm_409(Cluster),
    confirm_list(Cluster, [<<"test_index_1">>, <<"test_index_2">>, <<"test_index_409">>]),
    confirm_delete(Cluster, <<"test_index_1">>),
    confirm_get(Cluster, <<"test_index_2">>),
    confirm_404(Cluster, <<"not_an_index">>),
    confirm_delete_409(Cluster, <<"delete_409">>),
    pass.

%% @doc Test basic creation, no body.
confirm_create_index_1(Cluster) ->
    Index = <<"test_index_1">>,
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_create_index_1 ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    {ok, Status, _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("204", Status),
    yz_rt:wait_for_index(Cluster, Index).

%% @doc Test index creation when passing schema name.
confirm_create_index_2(Cluster) ->
    Index = <<"test_index_2">>,
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_create_index_2 ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    Headers = [{"content-type", "application/json"}],
    Body = <<"{\"schema\":\"_yz_default\"}">>,
    {ok, Status, _, _} = http(put, URL, Headers, Body),
    ?assertEqual("204", Status),
    yz_rt:wait_for_index(Cluster, Index).

confirm_409(Cluster) ->
    Index = <<"test_index_409">>,
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_409 ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    {ok, Status1, _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("204", Status1),
    yz_rt:wait_for_index(Cluster, Index),
    {ok, Status2, _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("409", Status2).

confirm_list(Cluster, Indexes) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_list ~p [~p]", [Indexes, HP]),
    URL = index_list_url(HP),
    {ok, "200", _, Body} = http(get, URL, ?NO_HEADERS, ?NO_BODY),
    check_list(Indexes, Body).

confirm_delete(Cluster, Index) ->
    confirm_get(Cluster, Index),
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_delete ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),

    Suffix = "yz/index/" ++ Index,
    Is200 = is_status("200", get, Suffix, ?NO_HEADERS, ?NO_BODY),
    [ ?assertEqual(ok, rt:wait_until(Node, Is200)) || Node <- Cluster],

    %% Only run DELETE on one node
    {ok, Status2, _, _} = http(delete, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("204", Status2),

    Is404 = is_status("404", get, Suffix, ?NO_HEADERS, ?NO_BODY),
    [ ?assertEqual(ok, rt:wait_until(Node, Is404)) || Node <- Cluster],

    %% Verify the index dir was removed from disk as well
    [ ?assertEqual(ok, rt:wait_until(Node, is_deleted("data/yz/" ++ binary_to_list(Index))))
      || Node <- Cluster].

is_status(ExpectedStatus, Method, URLSuffix, Headers, Body) ->
    fun(Node) ->
            HP = hd(host_entries(rt:connection_info([Node]))),
            URL = url(HP, URLSuffix),
            lager:info("checking for expected status ~p from ~p ~p",
                       [ExpectedStatus, Method, URL]),
            {ok, Status, _, _} = http(Method, URL, Headers, Body),
            ExpectedStatus == Status
    end.

is_deleted(Dir) ->
    fun(Node) ->
            lager:info("checking if dir ~p is deleted for node ~p", [Dir, Node]),
            not rpc:call(Node, filelib, is_dir, [Dir])
    end.

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

%% @doc Confirm that an index with 1 or more buckets associated with
%%      it is not allowed to be deleted.
confirm_delete_409(Cluster, Index) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("Verify that index ~s cannot be deleted b/c of associated buckets [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    {ok, "204", _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    H = [{"content-type", "application/json"}],
    B = <<"{\"props\":{\"yz_index\":\"delete_409\"}}">>,
    {ok, "204", _, _} = http(put, bucket_url(HP, <<"b1">>), H, B),
    {ok, "204", _, _} = http(put, bucket_url(HP, <<"b2">>), H, B),

    yz_rt:wait_for_index(Cluster, Index),
    %% Sleeping for bprops (TODO: convert to `wait_for_bprops')
    timer:sleep(4000),

    %% Verify associated_buckets is correct
    Node = select_random(Cluster),
    lager:info("Verify associated buckets for index ~s [~p]", [Node]),
    Ring = rpc:call(Node, yz_misc, get_ring, [transformed]),
    Assoc = rpc:call(Node, yz_index, associated_buckets, [Index, Ring]),
    ?assertEqual([<<"b1">>, <<"b2">>], Assoc),

    %% Can't be deleted because of associated buckets
    {ok, "409", _, _} = http(delete, URL, ?NO_HEADERS, ?NO_BODY),

    B2 = <<"{\"props\":{\"yz_index\":\"_yz_default\"}}">>,
    {ok, "204", _, _} = http(put, bucket_url(HP, <<"b2">>), H, B2),

    %% Still can't delete because of associated bucket
    {ok, "409", _, _} = http(delete, URL, ?NO_HEADERS, ?NO_BODY),

    B2 = <<"{\"props\":{\"yz_index\":\"_yz_default\"}}">>,
    {ok, "204", _, _} = http(put, bucket_url(HP, <<"b1">>), H, B2),

    %% TODO: wait_for_index_delete?
    {ok, "204", _, _} = http(delete, URL, ?NO_HEADERS, ?NO_BODY).

%%%===================================================================
%%% Helpers
%%%===================================================================

bucket_url({Host, Port}, Bucket) ->
    ?FMT("http://~s:~B/buckets/~s/props", [Host, Port, Bucket]).

check_list(Indexes, Body) ->
    Decoded = mochijson2:decode(Body),
    Names = [proplists:get_value(<<"name">>, Obj) || {struct, Obj} <- Decoded],
    ?assertEqual(lists:sort(Indexes), lists:sort(Names)).

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

url({Host,Port}, Suffix) ->
    ?FMT("http://~s:~B/~s", [Host, Port, Suffix]).

join(Nodes) ->
    [NodeA|Others] = Nodes,
    [rt:join(Node, NodeA) || Node <- Others],
    Nodes.

prepare_cluster(NumNodes) ->
    Nodes = rt:deploy_nodes(NumNodes, ?CFG),
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
