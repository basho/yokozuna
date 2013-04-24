-module(yz_rt).
-compile(export_all).

-spec connection_info(list()) -> orddict:orddict().
connection_info(Cluster) ->
    CI = orddict:from_list(rt:connection_info(Cluster)),
    SolrInfo = orddict:from_list([{Node, [{solr_http, get_yz_conn_info(Node)}]}
                                  || Node <- Cluster]),
    orddict:merge(fun(_,V1,V2) -> V1 ++ V2 end, CI, SolrInfo).

create_index(Node, Index) ->
    lager:info("Creating index ~s [~p]", [Index, Node]),
    ok = rpc:call(Node, yz_index, create, [Index]).

create_index(Node, Index, SchemaName) ->
    lager:info("Creating index ~s using schema ~s [~p]",
               [Index, SchemaName, Node]),
    ok = rpc:call(Node, yz_index, create, [Index, SchemaName]).

get_count(Resp) ->
    Struct = mochijson2:decode(Resp),
    yz_driver:get_path(Struct, [<<"response">>, <<"numFound">>]).

-spec get_yz_conn_info(node()) -> {string(), string()}.
get_yz_conn_info(Node) ->
    {ok, SolrPort} = rpc:call(Node, application, get_env, [yokozuna, solr_port]),
    %% Currently Yokozuna hardcodes listener to all interfaces
    {"127.0.0.1", SolrPort}.

host_entries(ClusterConnInfo) ->
    [riak_http(I) || {_,I} <- ClusterConnInfo].

load_data(Cluster, Index, YZBenchDir, NumKeys) ->
    lager:info("Load data for index ~p onto cluster ~p", [Index, Cluster]),
    Hosts = host_entries(rt:connection_info(Cluster)),
    KeyGen = {function, yz_driver, fruit_key_val_gen, [NumKeys]},
    Cfg = [{mode,max},
           {duration,5},
           {concurrent, 3},
           {code_paths, [YZBenchDir]},
           {driver, yz_driver},
           {index_path, "/riak/" ++ Index},
           {http_conns, Hosts},
           {pb_conns, []},
           {key_generator, KeyGen},
           {operations, [{load_fruit, 1}]}],
    File = "bb-load-" ++ Index,
    write_terms(File, Cfg),
    run_bb(sync, File).

random_keys(MaxKey) ->
    random_keys(random:uniform(100), MaxKey).

random_keys(Num, MaxKey) ->
    lists:usort([integer_to_list(random:uniform(MaxKey))
                 || _ <- lists:seq(1, Num)]).

riak_http(ConnInfo) ->
    proplists:get_value(http, ConnInfo).

run_bb(Method, File) ->
    Fun = case Method of
              sync -> cmd;
              async -> spawn_cmd
          end,
    rt:Fun("$YZ_BENCH_DIR/deps/basho_bench/basho_bench " ++ File).

search_expect(HP, Index, Name, Term, Expect) ->
    search_expect(yokozuna, HP, Index, Name, Term, Expect).

search_expect(Type, HP, Index, Name, Term, Expect) ->
    {ok, "200", _, R} = search(Type, HP, Index, Name, Term),
    verify_count(Expect, R).

search(HP, Index, Name, Term) ->
    search(yokozuna, HP, Index, Name, Term).

search(Type, {Host, Port}, Index, Name, Term) when is_integer(Port) ->
    search(Type, {Host, integer_to_list(Port)}, Index, Name, Term);

search(Type, {Host, Port}, Index, Name, Term) ->
    FmtStr = case Type of
                 solr ->
                     "http://~s:~s/solr/~s/select?q=~s:~s&wt=json";
                 yokozuna ->
                     "http://~s:~s/search/~s?q=~s:~s&wt=json"
             end,
    URL = lists:flatten(io_lib:format(FmtStr,
                                      [Host, Port, Index, Name, Term])),
    lager:info("Run search ~s", [URL]),
    Opts = [{response_format, binary}],
    ibrowse:send_req(URL, [], get, [], Opts).

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

set_index_flag(Node, Bucket) ->
    set_index_flag(Node, Bucket, true).

set_index_flag(Node, Bucket, Value) ->
    lager:info("Set index flag on bucket ~s [~p]", [Bucket, Node]),
    ok = rpc:call(Node, yz_kv, set_index_flag, [Bucket, Value]).

solr_http(ConnInfo) ->
    proplists:get_value(solr_http, ConnInfo).

verify_count(Expected, Resp) ->
    lager:info("E: ~p, A: ~p", [Expected, get_count(Resp)]),
    Expected == get_count(Resp).

wait_for_aae(Cluster, F) ->
    wait_for_aae(Cluster, F, 0).

wait_for_aae(_, _, 24) ->
    lager:error("Hit limit waiting for AAE"),
    aae_failed;
wait_for_aae(Cluster, F, Tries) ->
    case F(Cluster) of
        true -> ok;
        _ ->
            timer:sleep(5000),
            wait_for_aae(Cluster, F, Tries + 1)
    end.

wait_for_joins(Cluster) ->
    lager:info("Waiting for ownership handoff to finish"),
    rt:wait_until_nodes_ready(Cluster),
    rt:wait_until_no_pending_changes(Cluster).

write_terms(File, Terms) ->
    {ok, IO} = file:open(File, [write]),
    [io:fwrite(IO, "~p.~n", [T]) || T <- Terms],
    file:close(IO).
