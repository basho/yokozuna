%% @doc Test the index schema API in various ways.
-module(yz_wm_extract_test).
-compile(export_all).
-import(yz_rt, [host_entries/1, select_random/1]).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).

confirm() ->
    Cluster = prepare_cluster(4),
    confirm_check_if_registered_set_ct(Cluster),
    pass.

%% @doc Confirm yz-extractor header works as a string in
%%      yz_wm_extract:check_if_registered_set_ct/4
confirm_check_if_registered_set_ct(Cluster) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_check_if_registered_set_ct [~p]", [HP]),
    URL = extract_url(HP),
    Headers = [{?YZ_HEAD_EXTRACTOR,"yz_json_extractor"}],
    {ok, Status, _, _} = http(put, URL, Headers, "{\"name\":\"ryan\"}"),
    ?assertEqual("200", Status).

%%%===================================================================
%%% Helpers
%%%===================================================================

http(Method, URL, Headers, Body) ->
    Opts = [{response_format, binary}],
    ibrowse:send_req(URL, Headers, Method, Body, Opts).

extract_url({Host,Port}) ->
    ?FMT("http://~s:~B/extract", [Host, Port]).

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
    yz_rt:wait_for_joins(Cluster),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    Cluster.
