%% @doc Ensure that error messages return as expected
-module(yz_errors).
-compile(export_all).
-import(yz_rt, [host_entries/1,
                run_bb/2, search_expect/5,
                select_random/1, verify_count/2,
                write_terms/2]).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NO_HEADERS, []).
-define(CFG,
        [{riak_core,
          [
           {handoff_concurrency, 16},
           {inactivity_timeout, 1000},
           {ring_creation_size, 16}
          ]},
         {yokozuna,
          [
	   {enabled, true}
          ]}
        ]).

confirm() ->
    random:seed(now()),
    Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    ok = test_errors(Cluster),
    pass.

test_errors(Cluster) ->
    ok = expect_bad_json(Cluster),
    ok = expect_bad_xml(Cluster),
    ok = expect_bad_query(Cluster),
    ok.

expect_bad_json(Cluster) ->
    Index = <<"bad_json">>,
    Bucket = {<<"bad_json">>,<<"bucket">>},
    HP = yz_rt:select_random(host_entries(rt:connection_info(Cluster))),
    ok = yz_rt:create_index_http(Cluster, Index),
    lager:info("Write bad json [~p]", [HP]),
    URL = bucket_url(HP, Bucket, "test"),
    Opts = [],
    CT = "application/json",
    Headers = [{"content-type", CT}],
    Body = "{\"bad\": \"unclosed\"",
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body, Opts),
    %% Sleep for soft commit
    timer:sleep(1100),
    %% still store the value in riak
    {ok, "200", _, Body} = ibrowse:send_req(URL, [{"accept", CT}], get, []),
    %% Sleep for soft commit
    timer:sleep(1100),
    ?assert(search_expect(HP, Index, ?YZ_ERR_FIELD_S, "1", 1)),
    ok.

expect_bad_xml(Cluster) ->
    Index = <<"bad_xml">>,
    Bucket = {Index,<<"bucket">>},
    HP = yz_rt:select_random(host_entries(rt:connection_info(Cluster))),
    ok = yz_rt:create_index_http(Cluster, Index),
    lager:info("Write bad xml [~p]", [HP]),
    URL = bucket_url(HP, Bucket, "test"),
    Opts = [],
    CT = "application/xml",
    Headers = [{"content-type", CT}],
    Body = "<\"bad\" \"xml\"></",
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body, Opts),
    %% Sleep for soft commit
    timer:sleep(1100),
    %% still store the value in riak
    {ok, "200", _, Body} = ibrowse:send_req(URL, [{"accept", CT}], get, []),
    %% Sleep for soft commit
    timer:sleep(1100),
    ?assert(search_expect(HP, Index, ?YZ_ERR_FIELD_S, "1", 1)),
    ok.

expect_bad_query(Cluster) ->
    Index = <<"bad_query">>,
    Bucket = {Index, <<"bucket">>},
    HP = yz_rt:select_random(host_entries(rt:connection_info(Cluster))),
    ok = yz_rt:create_index_http(Cluster, Index),
    lager:info("Write bad query [~p]", [HP]),
    URL = bucket_url(HP, Bucket, "test"),
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
    SearchURL = search_url(HP, Index) ++ "?q=*:*&sort=sco+desc",
    {ok, "400", _, _} = ibrowse:send_req(SearchURL, [], get, []),
    ok.

bucket_url({Host,Port}, {BType, BName}, Key) ->
    ?FMT("http://~s:~B/types/~s/buckets/~s/keys/~s", [Host, Port, BType, BName, Key]).

search_url({Host,Port}, Index) ->
    ?FMT("http://~s:~B/solr/~s/select", [Host, Port, Index]).
