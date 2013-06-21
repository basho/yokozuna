-module(yz_rt).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("yokozuna.hrl").
-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).

-type host() :: string().
-type portnum() :: integer().

%% Copied from rt.erl, would be nice if there was a rt.hrl
-type interface() :: {http, tuple()} | {pb, tuple()}.
-type interfaces() :: [interface()].
-type conn_info() :: [{node(), interfaces()}].

-spec connection_info(list()) -> orddict:orddict().
connection_info(Cluster) ->
    CI = orddict:from_list(rt:connection_info(Cluster)),
    SolrInfo = orddict:from_list([{Node, [{solr_http, get_yz_conn_info(Node)}]}
                                  || Node <- Cluster]),
    orddict:merge(fun(_,V1,V2) -> V1 ++ V2 end, CI, SolrInfo).

-spec create_index(node(), index_name()) -> ok.
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

-spec host_entries(conn_info()) -> [{host(), portnum()}].
host_entries(ClusterConnInfo) ->
    [riak_http(I) || {_,I} <- ClusterConnInfo].

-spec http_put({string(), portnum()}, binary(), binary(), binary()) -> ok.
http_put(HP, Bucket, Key, Value) ->
    http_put(HP, Bucket, Key, "text/plain", Value).

-spec http_put({string(), portnum()}, binary(), binary(), string(), binary()) -> ok.
http_put({Host, Port}, Bucket, Key, CT, Value) ->
    URL = ?FMT("http://~s:~s/riak/~s/~s",
               [Host, integer_to_list(Port), Bucket, Key]),
    Opts = [],
    Headers = [{"content-type", CT}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Value, Opts),
    ok.

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

-spec riak_http(interfaces()) -> {host(), portnum()}.
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

remove_index(Node, Bucket) ->
    lager:info("Remove index from bucket ~s [~p]", [Bucket, Node]),
    ok = rpc:call(Node, yz_kv, remove_index, [Bucket]).

set_index(Node, Bucket) ->
    set_index(Node, Bucket, binary_to_list(Bucket)).

set_index(Node, Bucket, Index) ->
    lager:info("Set bucket ~s index to ~s [~p]", [Bucket, Index, Node]),
    ok = rpc:call(Node, yz_kv, set_index, [Bucket, Index]).

solr_http(ConnInfo) ->
    proplists:get_value(solr_http, ConnInfo).

verify_count(Expected, Resp) ->
    lager:info("E: ~p, A: ~p", [Expected, get_count(Resp)]),
    Expected == get_count(Resp).

-spec wait_for_index(list(), string()) -> term().
wait_for_index(Cluster, Index) ->
    IsIndexUp =
        fun(Node) ->
                lager:info("Waiting for index ~s to be avaiable on node ~p", [Index, Node]),
                rpc:call(Node, yz_solr, ping, [Index])
        end,
    [?assertEqual(ok, rt:wait_until(Node, IsIndexUp)) || Node <- Cluster].

wait_for_joins(Cluster) ->
    lager:info("Waiting for ownership handoff to finish"),
    rt:wait_until_nodes_ready(Cluster),
    rt:wait_until_no_pending_changes(Cluster).

write_terms(File, Terms) ->
    {ok, IO} = file:open(File, [write]),
    [io:fwrite(IO, "~p.~n", [T]) || T <- Terms],
    file:close(IO).

%% @doc Wrapper around `rt:wait_until' to verify `F' against multiple
%%      nodes.  The function `F' is passed one of the `Nodes' as
%%      argument and must return a `boolean()' delcaring whether the
%%      success condition has been met or not.
-spec wait_until([node()], fun((node()) -> boolean())) -> ok.
wait_until(Nodes, F) ->
    [?assertEqual(ok, rt:wait_until(Node, F)) || Node <- Nodes],
    ok.
