-module(yz_rt).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("yokozuna.hrl").
-define(YZ_RT_ETS, yz_rt_ets).
-define(YZ_RT_ETS_OPTS, [public, named_table, {write_concurrency, true}]).
-define(NO_BODY, <<>>).
-define(IBROWSE_TIMEOUT, 60000).
-define(SOFTCOMMIT, 1000).

-type host() :: string().
-type portnum() :: integer().

%% Copied from rt.erl, would be nice if there was a rt.hrl
-type interface() :: {http, tuple()} | {pb, tuple()}.
-type interfaces() :: [interface()].
-type conn_info() :: [{node(), interfaces()}].
-type prop() :: {atom(), any()}.
-type props() :: [prop()].

-type cluster() :: [node()].

%% @doc Given a list of protobuff connections, close each one.
%%
%% @see open_pb_conns/1
-spec close_pb_conns([pid()]) -> ok.
close_pb_conns(PBConns) ->
    [riakc_pb_socket:stop(C) || C <- PBConns],
    ok.

-spec connection_info(list()) -> orddict:orddict().
connection_info(Cluster) ->
    CI = orddict:from_list(rt:connection_info(Cluster)),
    SolrInfo = orddict:from_list([{Node, [{solr_http, get_yz_conn_info(Node)}]}
                                  || Node <- Cluster]),
    orddict:merge(fun(_,V1,V2) -> V1 ++ V2 end, CI, SolrInfo).

-spec create_bucket_type(node(), binary()) -> ok.
create_bucket_type(Node, BucketType) ->
    create_bucket_type(Node, BucketType, []).

-spec create_bucket_type(node(), binary(), [term()]) -> ok.
create_bucket_type(Node, BucketType, Props) ->
    rt:create_and_activate_bucket_type(Node, BucketType, Props),
    rt:wait_until_bucket_type_status(BucketType, active, Node).

-spec create_index(node(), index_name()) -> ok.
create_index(Node, Index) ->
    lager:info("Creating index ~s [~p]", [Index, Node]),
    ok = rpc:call(Node, yz_index, create, [Index]).

-spec create_index(node(), index_name(), schema_name()) -> ok.
create_index(Node, Index, SchemaName) ->
    lager:info("Creating index ~s with schema ~s [~p]",
               [Index, SchemaName, Node]),
    ok = rpc:call(Node, yz_index, create, [Index, SchemaName]).

-spec create_index(node(), index_name(), schema_name(), n()) -> ok.
create_index(Node, Index, SchemaName, NVal) ->
    lager:info("Creating index ~s with schema ~s and n_val: ~p [~p]",
               [Index, SchemaName, NVal, Node]),
    ok = rpc:call(Node, yz_index, create, [Index, SchemaName, NVal]).

-spec create_index_http(cluster(), index_name()) -> ok.
create_index_http(Cluster, Index) ->
    Node = yz_rt:select_random(Cluster),
    HP = hd(host_entries(rt:connection_info([Node]))),
    create_index_http(Cluster, HP, Index).

-spec create_index_http(cluster(), {string(), portnum()}, index_name()) -> ok.
create_index_http(Cluster, HP, Index) ->
    Node = hd(Cluster),
    URL = yz_rt:index_url(HP, Index),
    Headers = [{"content-type", "application/json"}],
    lager:info("create_index ~s [~p]", [Index, Node]),
    {ok, "204", _, _} = yz_rt:http(put, URL, Headers, ?NO_BODY),
    yz_rt:set_bucket_type_index(Node, Index),
    yz_rt:wait_for_bucket_type(Cluster, Index).

maybe_create_ets() ->
    case ets:info(?YZ_RT_ETS) of
        undefined ->
            ets:new(?YZ_RT_ETS, ?YZ_RT_ETS_OPTS),
            ok;
        _ ->
            ets:delete(?YZ_RT_ETS),
            ets:new(?YZ_RT_ETS, ?YZ_RT_ETS_OPTS),
            ok
    end.

get_call_count(Cluster, MFA) when is_list(Cluster) ->
    case ets:lookup(?YZ_RT_ETS, MFA) of
        [{_,Count}] ->
            Count;
        [] ->
            0
    end.

-spec count_calls([node()], {atom(), atom(), non_neg_integer()}) -> ok.
count_calls(Cluster, MFA={M,F,A}) when is_list(Cluster) ->
    lager:info("count all calls to MFA ~p across the cluster ~p",
               [MFA, Cluster]),
    RiakTestNode = node(),
    maybe_create_ets(),
    dbg:tracer(process, {fun trace_count/2, {RiakTestNode, MFA, 0}}),
    [{ok,Node} = dbg:n(Node) || Node <- Cluster],
    dbg:p(all, call),
    dbg:tpl(M, F, A, [{'_', [], [{return_trace}]}]),
    ok.

-spec stop_tracing() -> ok.
stop_tracing() ->
    lager:info("stop all dbg tracing"),
    dbg:stop_clear(),
    ok.

trace_count({trace, _Pid, call, {_M, _F, _A}}, Acc) ->
    Acc;
trace_count({trace, _Pid, return_from, {_M, _F, _}, _Result}, {RTNode, MFA, Count}) ->
    Count2 = Count + 1,
    rpc:call(RTNode, ets, insert, [?YZ_RT_ETS, {MFA, Count2}]),
    {RTNode, MFA, Count2}.

get_count(Resp) ->
    Struct = mochijson2:decode(Resp),
    kvc:path([<<"response">>, <<"numFound">>], Struct).

-spec get_yz_conn_info(node()) -> {string(), string()}.
get_yz_conn_info(Node) ->
    {ok, SolrPort} = rpc:call(Node, application, get_env, [yokozuna, solr_port]),
    %% Currently Yokozuna hardcodes listener to all interfaces
    {"127.0.0.1", SolrPort}.

-spec host_entries(conn_info()) -> [{host(), portnum()}].
host_entries(ClusterConnInfo) ->
    [riak_http(I) || {_,I} <- ClusterConnInfo].

-spec http(ibrowse:method(), string(), list(), string()) -> ibrowse:response().
http(Method, URL, Headers, Body) ->
    Opts = [],
    ibrowse:send_req(URL, Headers, Method, Body, Opts, ?IBROWSE_TIMEOUT).

-spec http(ibrowse:method(), string(), list(), string(), list()|timeout())
          -> ibrowse:response().
http(Method, URL, Headers, Body, Opts) when is_list(Opts)  ->
    ibrowse:send_req(URL, Headers, Method, Body, Opts, ?IBROWSE_TIMEOUT);
http(Method, URL, Headers, Body, Timeout) when is_integer(Timeout) ->
    Opts = [],
    ibrowse:send_req(URL, Headers, Method, Body, Opts, Timeout).

-spec http(ibrowse:method(), string(), list(), string(), list(), timeout())
          -> ibrowse:response().
http(Method, URL, Headers, Body, Opts, Timeout) when
      is_list(Opts) andalso is_integer(Timeout) ->
    ibrowse:send_req(URL, Headers, Method, Body, Opts, Timeout).

-spec http_put({string(), portnum()}, bucket(), binary(), binary()) -> ok.
http_put(HP, Bucket, Key, Value) ->
    http_put(HP, Bucket, Key, "text/plain", Value).

-spec http_put({string(), portnum()}, bucket(), binary(), string(), binary()) -> ok.
http_put({Host, Port}, {BType, BName}, Key, CT, Value) ->
    URL = ?FMT("http://~s:~s/types/~s/buckets/~s/keys/~s",
               [Host, integer_to_list(Port), BType, BName, Key]),
    Opts = [],
    Headers = [{"content-type", CT}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Value, Opts),
    ok.

-spec schema_url({string(), portnum()}, schema_name()) -> ok.
schema_url({Host,Port}, Name) ->
    ?FMT("http://~s:~B/search/schema/~s", [Host, Port, Name]).

-spec index_url({string(), portnum()}, index_name()) -> ok.
index_url({Host,Port}, Index) ->
    ?FMT("http://~s:~B/search/index/~s", [Host, Port, Index]).
index_url({Host, Port}, Index, Timeout) ->
    ?FMT("http://~s:~B/search/index/~s?timeout=~B", [Host, Port, Index,
                                                     Timeout]).

-spec search_url({string(), portnum()}, index_name()) -> ok.
search_url({Host, Port}, Index) ->
    ?FMT("http://~s:~B/search/query/~s", [Host, Port, Index]).

%% @doc Run basho bench job to load fruit data on `Cluster'.
%%
%% `Cluster' - List of nodes to send requests to.
%%
%% `Bucket' - The bucket name to write data to.
%%
%% `YZBenchDir' - The file path to Yokozuna's `misc/bench' dir.
%%
%% `NumKeys' - The total number of keys to write.  The maximum values
%% is 10 million.
%% `Operations` - Basho Bench operations to run.
-spec load_data(cluster(), bucket(), string(), integer(), [tuple()]) ->
                       timeout |
                       {Status :: integer(), Output :: binary()}.
load_data(Cluster, Bucket, YZBenchDir, NumKeys, Operations) ->
    lager:info("Load data into bucket ~p onto cluster ~p", [Bucket, Cluster]),
    Hosts = host_entries(rt:connection_info(Cluster)),
    KeyGen = {function, yz_driver, fruit_key_val_gen, [NumKeys]},
    Cfg = [{mode,max},
           {duration,7},
           {concurrent, 3},
           {code_paths, [YZBenchDir]},
           {driver, yz_driver},
           {bucket, Bucket},
           {http_conns, Hosts},
           {pb_conns, []},
           {key_generator, KeyGen},
           {operations, Operations},
           {shutdown_on_error, true}],
    File = "load-data",
    write_terms(File, Cfg),
    run_bb(sync, File).

-spec load_data(cluster(), bucket(), string(), integer()) ->
                       timeout |
                       {Status :: integer(), Output :: binary()}.
load_data(Cluster, Bucket, YZBenchDir, NumKeys) ->
    load_data(Cluster, Bucket, YZBenchDir, NumKeys, [{load_fruit, 1}]).

%% @doc Load the BEAM for `Module' across the `Nodes'.  This allows
%% use of higher order functions, defined in a riak test module, on
%% the Riak node.
%%
%% @see yz_mapreduce:collect_results/2
-spec load_module([node()], module()) -> ok.
load_module(Nodes, Module) ->
    {Mod, Bin, File} = code:get_object_code(Module),
    {_, []} = rpc:multicall(Nodes, code, load_binary, [Mod, File, Bin]),
    ok.

%% @doc Open a protobuff connection to each node and return the list
%% of connections.
%%
%% @see close_pb_conns/1
-spec open_pb_conns(cluster()) -> [PBConn :: pid()].
open_pb_conns(Cluster) ->
    [begin
         {Host, Port} = riak_pb(CI),
         {ok, PBConn} = riakc_pb_socket:start_link(Host, Port),
         PBConn
     end || {_Node, CI} <- rt:connection_info(Cluster)].

-spec random_keys(pos_integer()) -> [binary()].
random_keys(MaxKey) ->
    random_keys(4 + random:uniform(100), MaxKey).

-spec random_keys(pos_integer(), pos_integer()) -> [binary()].
random_keys(Num, MaxKey) ->
    lists:usort([?INT_TO_BIN(random:uniform(MaxKey))
                 || _ <- lists:seq(1, Num)]).

-spec riak_http({node(), interfaces()} | interfaces()) -> {host(), portnum()}.
riak_http({_Node, ConnInfo}) ->
    riak_http(ConnInfo);
riak_http(ConnInfo) ->
    proplists:get_value(http, ConnInfo).

-spec riak_pb({node(), interfaces()} | interfaces()) -> {host(), portnum()}.
riak_pb({_Node, ConnInfo}) ->
    riak_pb(ConnInfo);
riak_pb(ConnInfo) ->
    proplists:get_value(pb, ConnInfo).

run_bb(Method, File) ->
    Fun = case Method of
              sync -> cmd;
              async -> spawn_cmd
          end,
    AbsFile = filename:absname(File),
    BB = filename:join([rt_config:get(basho_bench), "basho_bench"]),
    Path = lists:flatten([BB, " -d /tmp/yz-bb-results ", AbsFile]),
    lager:debug("Executing b_b with the following command: ~p", [Path]),
    rt:Fun(Path).

-spec bb_driver_setup() -> {ok, string()} | {error, atom()}.
bb_driver_setup() ->
    YZBenchDir = rt_config:get(yz_dir) ++ "/misc/bench",
    YZDriverSrc = filename:join([YZBenchDir, "src"]),
    YZDriverEbin = filename:join([YZBenchDir, "ebin"]),
    BBDir = rt_config:get(basho_bench),
    clean_dir(YZDriverEbin),
    case build_bb_driver(YZDriverSrc, BBDir, YZDriverEbin) of
        true ->
            {ok, YZBenchDir};
        false ->
            {error, bb_driver_build_failed}
    end.

-spec build_bb_driver(string(), string(), string()) -> boolean().
build_bb_driver(SrcDir, BBDir, OutputDir) ->
    Files = ["yz_driver.erl", "yz_file_terms.erl"],
    Options = [{i, BBDir  ++ "/../"}, {outdir, OutputDir}, debug_info,
               return_errors, return_warnings,
               {parse_transform, lager_transform}],
    BuildRes = [case compile:file(SrcDir ++ "/" ++ File, Options) of
                    {ok,_,_} ->
                        true;
                    Err ->
                        lager:error("Error compiling file ~s ~p", [File, Err]),
                        false
                end
                || File <- Files],
    lists:all(fun(X) -> X end, BuildRes).

clean_dir("/") ->
    lager:info("Sorry, this is a testing tool. Do your own dirty work!"),
    ok;
clean_dir(Dir) ->
    os:cmd(io_lib:format("rm -rf ~s", [Dir])),
    os:cmd(io_lib:format("mkdir -p ~s", [Dir])).

search_expect(HP, Index, Name, Term, Expect) ->
    search_expect(yokozuna, HP, Index, Name, Term, Expect).

search_expect(Type, HP, Index, Name, Term, Expect) ->
    {ok, "200", _, R} = search(Type, HP, Index, Name, Term),
    verify_count(Expect, R).

search_expect(solr, {Host, Port}, Index, Name0, Term0, Shards, Expect)
  when is_list(Shards), length(Shards) > 0 ->
    Name = quote_unicode(Name0),
    Term = quote_unicode(Term0),
    URL = internal_solr_url(Host, Port, Index, Name, Term, Shards),
    lager:info("Run search ~s", [URL]),
    Opts = [{response_format, binary}],
    {ok, "200", _, R} = ibrowse:send_req(URL, [], get, [], Opts),
    verify_count(Expect, R).

search(HP, Index, Name, Term) ->
    search(yokozuna, HP, Index, Name, Term).

search(Type, {Host, Port}, Index, Name, Term) when is_integer(Port) ->
    search(Type, {Host, integer_to_list(Port)}, Index, Name, Term);

search(Type, {Host, Port}, Index, Name0, Term0) ->
    Name = quote_unicode(Name0),
    Term = quote_unicode(Term0),
    FmtStr = case Type of
                 solr ->
                     "http://~s:~s/internal_solr/~s/select?q=~s:~s&wt=json";
                 yokozuna ->
                     "http://~s:~s/search/query/~s?q=~s:~s&wt=json"
             end,
    URL = ?FMT(FmtStr, [Host, Port, Index, Name, Term]),
    lager:info("Run search ~s", [URL]),
    Opts = [{response_format, binary}],
    ibrowse:send_req(URL, [], get, [], Opts).

quote_unicode(Value) ->
    mochiweb_util:quote_plus(binary_to_list(unicode:characters_to_binary(Value))).

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

%% @doc Associate the `Index' with the `Bucket', sending the request
%% to `Node'.
-spec set_index(node(), bucket(), index_name()) -> ok | {error, any()}.
set_index(Node, Bucket, Index) ->
    Props = [{?YZ_INDEX, Index}],
    set_bucket_props(Node, Bucket, Props).

-spec set_index(node(), bucket(), index_name(), n()) -> ok | {error, any()}.
set_index(Node, Bucket, Index, NVal) ->
    Props = [{?YZ_INDEX, Index}, {n_val, NVal}],
    set_bucket_props(Node, Bucket, Props).

-spec set_bucket_props(node(), bucket(), props()) -> ok | {error, any()}.
set_bucket_props(Node, Bucket, Props) ->
    rpc:call(Node, riak_core_bucket, set_bucket, [Bucket, Props]).

-spec remove_index(node(), binary()) -> ok.
remove_index(Node, BucketType) ->
    lager:info("Remove index from bucket type ~s [~p]", [BucketType, Node]),
    Props = [{?YZ_INDEX, ?YZ_INDEX_TOMBSTONE}],
    ok = rpc:call(Node, riak_core_bucket_type, update, [BucketType, Props]).

set_bucket_type_index(Node, BucketType) ->
    set_bucket_type_index(Node, BucketType, BucketType).

set_bucket_type_index(Node, BucketType, Index) ->
    lager:info("Set bucket type ~s index to ~s [~p]", [BucketType, Index, Node]),
    create_bucket_type(Node, BucketType, [{?YZ_INDEX, Index}]).

set_bucket_type_index(Node, BucketType, Index, NVal) ->
    lager:info("Set bucket type ~s index to ~s [~p]", [BucketType, Index, Node]),
    create_bucket_type(Node, BucketType, [{?YZ_INDEX, Index},{n_val,NVal}]).

solr_http({_Node, ConnInfo}) ->
    solr_http(ConnInfo);
solr_http(ConnInfo) ->
    proplists:get_value(solr_http, ConnInfo).

%% @doc Store the schema under `Name' using the protobuff `PB'
%% connection.
-spec store_schema(pid(), schema_name(), raw_schema()) -> ok.
store_schema(PBConn, Name, Raw) ->
    lager:info("Storing schema ~s", [Name]),
    ?assertEqual(ok, riakc_pb_socket:create_search_schema(PBConn, Name, Raw)),
    ok.

%% @doc Wait for all AAE trees to be built.
-spec wait_for_all_trees([node()]) -> ok.
wait_for_all_trees(Cluster) ->
    F = fun(Node) ->
                lager:info("Check if all trees built for node ~p", [Node]),
                Info = rpc:call(Node, yz_kv, compute_tree_info, []),
                NotBuilt = [X || {_,undefined}=X <- Info],
                NotBuilt == []
        end,
    yz_rt:wait_until(Cluster, F),
    ok.

wait_for_bucket_type(Cluster, BucketType) ->
    F = fun(Node) ->
                {Host, Port} = riak_pb(hd(rt:connection_info([Node]))),
                {ok, PBConn} = riakc_pb_socket:start_link(Host, Port),
                R = riakc_pb_socket:get_bucket_type(PBConn, BucketType),
                case R of
                    {ok,_} -> true;
                    _ -> false
                end
        end,
    wait_until(Cluster, F),
    ok.

%% @doc Wait for a full exchange round since `Timestamp'.  This means
%% that all `{Idx,N}' for all partitions must have exchanged after
%% `Timestamp'.
-spec wait_for_full_exchange_round([node()], os:now()) -> ok.
wait_for_full_exchange_round(Cluster, Timestamp) ->
    lager:info("wait for full AAE exchange round on cluster ~p", [Cluster]),
    MoreRecent =
        fun({_Idx, _, undefined, _RepairStats}) ->
                false;
           ({_Idx, _, AllExchangedTime, _RepairStats}) ->
                AllExchangedTime > Timestamp
        end,
    AllExchanged =
        fun(Node) ->
                Exchanges = rpc:call(Node, yz_kv, compute_exchange_info, []),
                {_Recent, WaitingFor1} = lists:partition(MoreRecent, Exchanges),
                WaitingFor2 = [element(1,X) || X <- WaitingFor1],
                lager:info("Still waiting for AAE of ~p ~p", [Node, WaitingFor2]),
                [] == WaitingFor2
        end,
    yz_rt:wait_until(Cluster, AllExchanged),
    ok.

%% @see wait_for_schema/3
wait_for_schema(Cluster, Name) ->
    wait_for_schema(Cluster, Name, ignore).

%% @doc Wait for the schema `Name' to be read by all nodes in
%% `Cluster' before returning.  If `Content' is binary data when
%% verify the schema bytes exactly match `Content'.
-spec wait_for_schema(cluster(), schema_name(), ignore | raw_schema()) -> ok.
wait_for_schema(Cluster, Name, Content) ->
    F = fun(Node) ->
                lager:info("Attempt to read schema ~s from node ~p", [Name, Node]),
                {Host, Port} = riak_pb(hd(rt:connection_info([Node]))),
                {ok, PBConn} = riakc_pb_socket:start_link(Host, Port),
                R = riakc_pb_socket:get_search_schema(PBConn, Name),
                riakc_pb_socket:stop(PBConn),
                case R of
                    {ok, PL} ->
                        case Content of
                            ignore ->
                                Name == proplists:get_value(name, PL);
                            _ ->
                                (Name == proplists:get_value(name, PL)) and
                                    (Content == proplists:get_value(content, PL))
                        end;
                    _ ->
                        false
                end
        end,
    wait_until(Cluster,  F),
    ok.

verify_count(Expected, Resp) ->
    Count = get_count(Resp),
    lager:info("E: ~p, A: ~p", [Expected, Count]),
    Expected =:= get_count(Resp).

-spec wait_for_index(list(), index_name()) -> ok.
wait_for_index(Cluster, Index) ->
    IsIndexUp =
        fun(Node) ->
                lager:info("Waiting for index ~s to be avaiable on node ~p", [Index, Node]),
                rpc:call(Node, yz_solr, ping, [Index])
        end,
    [?assertEqual(ok, rt:wait_until(Node, IsIndexUp)) || Node <- Cluster],
    ok.

join_all(Nodes) ->
    [NodeA|Others] = Nodes,
    [rt:join(Node, NodeA) || Node <- Others],
    Nodes.

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

-spec node_solr_port(node()) -> port().
node_solr_port(Node) ->
    {ok, P} = riak_core_util:safe_rpc(Node, application, get_env,
                                      [yokozuna, solr_port]),
    P.

internal_solr_url(Host, Port, Index) ->
    ?FMT("http://~s:~B/internal_solr/~s", [Host, Port, Index]).
internal_solr_url(Host, Port, Index, Shards) ->
    internal_solr_url(Host, Port, Index, Shards, <<"*">>, <<"*">>).
internal_solr_url(Host, Port, Index, Name, Term, Shards) ->
    Ss = [internal_solr_url(Host, ShardPort, Index)
          || {_, ShardPort} <- Shards],
    ?FMT("http://~s:~B/internal_solr/~s/select?wt=json&q=~s:~s&shards=~s",
         [Host, Port, Index, Name, Term, string:join(Ss, ",")]).

-spec commit([node()], index_name()) -> ok.
commit(Nodes, Index) ->
    %% Wait for yokozuna index to trigger, then force a commit
    timer:sleep(?SOFTCOMMIT),
    lager:info("Commit search writes to ~s at softcommit (default) ~p",
               [Index, ?SOFTCOMMIT]),
    rpc:multicall(Nodes, yz_solr, commit, [Index]),
    ok.

entropy_data_url({Host, Port}, Index, Params) ->
    ?FMT("http://~s:~B/internal_solr/~s/entropy_data?~s",
         [Host, Port, Index, mochiweb_util:urlencode(Params)]).

-spec merge_config(proplist(), proplist()) -> proplist().
merge_config(Change, Base) ->
    lists:ukeymerge(1, lists:keysort(1, Change), lists:keysort(1, Base)).

-spec write_objs([node()], bucket()) -> ok.
write_objs(Cluster, Bucket) ->
    lager:info("Writing 1000 objects"),
    lists:foreach(write_obj(Cluster, Bucket), lists:seq(1,1000)).

-spec write_obj([node()], bucket()) -> fun().
write_obj(Cluster, Bucket) ->
    fun(N) ->
            PL = [{name_s,<<"yokozuna">>}, {num_i,N}],
            Key = list_to_binary(io_lib:format("key_~B", [N])),
            Body = mochijson2:encode(PL),
            HP = yz_rt:select_random(yz_rt:host_entries(rt:connection_info(
                                                          Cluster))),
            CT = "application/json",
            lager:info("Writing object with bkey ~p [~p]", [{Bucket, Key}, HP]),
            yz_rt:http_put(HP, Bucket, Key, CT, Body)
    end.
