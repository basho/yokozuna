%% @doc Verify that fallback data is handled properly.  I.e. not indexed.
-module(yz_fallback).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-define(NUM_KEYS, 1000).
-define(INDEX, <<"fallback">>).
-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 16}
          ]},
	 {yokozuna,
	  [
	   {enabled, true}
	  ]}
        ]).

confirm() ->
    YZBenchDir = rt_config:get_os_env("YZ_BENCH_DIR"),
    code:add_path(filename:join([YZBenchDir, "ebin"])),
    Cluster = rt:build_cluster(2, ?CFG),
    create_index(Cluster, ?INDEX),
    Cluster2 = take_node_down(Cluster),
    write_obj(Cluster2),
    check_fallbacks(Cluster2),
    HP = riak_hp(yz_rt:select_random(Cluster2), Cluster2),
    ?assert(yz_rt:search_expect(yokozuna, HP, ?INDEX, "*", "*", 1)),
    pass.

check_fallbacks(Cluster) ->
    Node = yz_rt:select_random(Cluster),
    KVPreflist = kv_preflist(Node, ?INDEX, ?INDEX),
    FallbackPreflist = filter_fallbacks(KVPreflist),
    LogicalFallbackPL = make_logical(Node, FallbackPreflist),
    [begin
         {H, P} = solr_hp(FNode, Cluster),
         ?assert(yz_rt:search_expect(solr, {H, P}, ?INDEX, "_yz_pn", integer_to_list(LPN), 0))
     end
     || {LPN, FNode} <- LogicalFallbackPL].

create_index(Cluster, Index) ->
    Node = yz_rt:select_random(Cluster),
    yz_rt:create_index(Node, Index),
    ok = yz_rt:set_index(Node, Index),
    timer:sleep(5000).

make_logical(Node, Preflist) ->
    rpc:call(Node, yz_misc, convert_preflist, [Preflist, logical]).

filter_fallbacks(Preflist) ->
    [PartitionNode || {{_,_} = PartitionNode, fallback} <- Preflist].

kv_preflist(Node, Bucket, Key) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    BucketProps = rpc:call(Node, riak_core_bucket, get_bucket, [Bucket, Ring]),
    DocIdx = rpc:call(Node, riak_core_util, chash_key, [{Bucket,Key}]),
    N = proplists:get_value(n_val,BucketProps),
    UpNodes = rpc:call(Node, riak_core_node_watcher, nodes, [riak_kv]),
    riak_core_apl:get_apl_ann(DocIdx, N, Ring, UpNodes).

solr_hp(Node, Cluster) ->
    CI = yz_rt:connection_info(Cluster),
    yz_rt:solr_http(proplists:get_value(Node, CI)).

take_node_down(Cluster) ->
    DownNode = yz_rt:select_random(Cluster),
    rt:stop(DownNode),
    timer:sleep(5000),
    Cluster -- [DownNode].

write_obj(Cluster) ->
    Node = yz_rt:select_random(Cluster),
    {Host, Port} = riak_hp(Node, Cluster),
    lager:info("write obj to node ~p", [Node]),
    URL = ?FMT("http://~s:~s/buckets/~s/keys/~s",
               [Host, integer_to_list(Port), ?INDEX, ?INDEX]),
    Headers = [{"content-type", "text/plain"}],
    Body = <<"yokozuna">>,
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body, []),
    timer:sleep(1100).

riak_hp(Node, Cluster) ->
    CI = yz_rt:connection_info(Cluster),
    yz_rt:riak_http(proplists:get_value(Node, CI)).
