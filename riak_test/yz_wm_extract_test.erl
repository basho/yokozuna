%% @doc Test the extractor API in various ways.
-module(yz_wm_extract_test).
-compile(nowarn_export_all).
-compile(export_all).
-import(yz_rt, [host_entries/1, select_random/1]).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

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
    Cluster = rt:deploy_nodes(1, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    confirm_check_if_registered_set_ct(Cluster),
    confirm_extract_on_content_type(Cluster),
    pass.

%% @doc Confirm yz-extractor header works as a string in
%%      yz_wm_extract:check_if_registered_set_ct/4
confirm_check_if_registered_set_ct(Cluster) ->
    HP = hd(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_check_if_registered_set_ct [~p]", [HP]),
    URL = extract_url(HP),
    Headers = [{?YZ_HEAD_EXTRACTOR,"yz_json_extractor"}],
    {ok, Status, _, _} = http(put, URL, Headers, "{\"name\":\"ryan\"}"),
    ?assertEqual("200", Status).

%% @doc Confirm that the extractor works based on content-type.
confirm_extract_on_content_type(Cluster) ->
    HP = hd(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_check_if_registered_set_ct [~p]", [HP]),
    URL = extract_url(HP),
    Headers = [{"content-type", "application/json"}],
    {ok, Status, _, Body} = http(put, URL, Headers, <<"{\"name\":\"ryan\"}">>),
    ?assertEqual("200", Status),
    ?assert(size(Body) > 0).

%%%===================================================================
%%% Helpers
%%%===================================================================

http(Method, URL, Headers, Body) ->
    Opts = [{response_format, binary}],
    ibrowse:send_req(URL, Headers, Method, Body, Opts).

extract_url({Host,Port}) ->
    ?FMT("http://~s:~B/search/extract", [Host, Port]).
