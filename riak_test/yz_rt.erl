-module(yz_rt).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("yokozuna.hrl").
-define(YZ_RT_ETS, yz_rt_ets).
-define(YZ_RT_ETS_OPTS, [public, named_table, {write_concurrency, true}]).
-define(NO_BODY, <<>>).
-define(IBROWSE_TIMEOUT, 60000).
-define(SOFTCOMMIT, 1000).

-type host() :: string()|atom().
-type portnum() :: non_neg_integer()|port().
-type method() :: get | post | head | options | put | delete | trace |
                  mkcol | propfind | proppatch | lock | unlock | move | copy.
-type response() :: {ok, string(), [{string(), string()}], string()|binary()} |
                    {error, term()}.
-type json_string() :: atom | string() | binary().

-export_type([host/0, portnum/0]).

%% Copied from rt.erl, would be nice if there was a rt.hrl
-type interface() :: {http, tuple()} | {pb, tuple()}.
-type interfaces() :: [interface()].
-type conn_info() :: [{node(), interfaces()}].
-type prop() :: {atom(), any()}.
-type props() :: [prop()].
-type search_type() :: solr | yokozuna.
-type cluster() :: cluster().

-type intercept() :: {{TargetFunctionName::atom(), TargetArity::non_neg_integer()}, InterceptFunctionName::atom()}.
-type intercepts() :: [intercept()].

-export_type([prop/0, props/0, cluster/0]).

%% @doc Get {Host, Port} from `Cluster'.
-spec host_port(cluster()) -> {host(), portnum()}.
host_port(Cluster) ->
    hd(host_entries(rt:connection_info(Cluster))).

%% @doc Given a list of protobuff connections, close each one.
%%
%% @see open_pb_conns/1
-spec close_pb_conns([pid()]) -> ok.
close_pb_conns(PBConns) ->
    [riakc_pb_socket:stop(C) || C <- PBConns],
    ok.

%% @doc Prepare a cluster of `NumNodes' nodes using the given `Config', wait for
%% all the nodes to join the cluster, and wait for the yokozuna service to start.
-spec prepare_cluster(NumNodes :: non_neg_integer(), Config :: props())
                     -> cluster().
prepare_cluster(NumNodes, Config) ->
    Nodes = rt:deploy_nodes(NumNodes, Config),
    Cluster = join_all(Nodes),
    wait_for_joins(Cluster),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    lager:info("Cluster ~p is prepared.", [Cluster]),
    Cluster.

-spec connection_info(list()) -> orddict:orddict().
connection_info(Cluster) ->
    CI = orddict:from_list(rt:connection_info(Cluster)),
    SolrInfo = orddict:from_list([{Node, [{solr_http, get_yz_conn_info(Node)}]}
                                  || Node <- Cluster]),
    orddict:merge(fun(_,V1,V2) -> V1 ++ V2 end, CI, SolrInfo).

-spec create_bucket_type(cluster(), binary()) -> ok.
create_bucket_type(Cluster, BucketType) ->
    create_bucket_type(Cluster, BucketType, []).

-spec create_bucket_type(cluster(), binary(), [term()]) -> ok.
create_bucket_type(Cluster, BucketType, Props) ->
    Node = select_random(Cluster),
    rt:create_and_activate_bucket_type(Node, BucketType, Props),
    rt:wait_until_bucket_type_status(BucketType, active, Node),
    rt:wait_until_bucket_type_visible(Cluster, BucketType).

-spec create_indexed_bucket_type(cluster(), binary(), index_name()) -> ok.
create_indexed_bucket_type(Cluster, BucketType, IndexName) ->
    create_indexed_bucket_type(Cluster, BucketType, IndexName, []).

-spec create_indexed_bucket_type(cluster(), binary(), index_name(), proplist()|schema_name()) -> ok.
create_indexed_bucket_type(Cluster, BucketType, IndexName, Properties) when is_list(Properties) ->
    NVal = proplists:get_value(n_val, Properties, 3),
    ok = create_index(Cluster, IndexName, ?YZ_DEFAULT_SCHEMA_NAME, NVal),
    ok = create_bucket_type(Cluster, BucketType, [{?YZ_INDEX, IndexName} | Properties]);
create_indexed_bucket_type(Cluster, BucketType, IndexName, SchemaName) ->
    ok = create_index(Cluster, IndexName, SchemaName),
    ok = create_bucket_type(Cluster, BucketType, [{?YZ_INDEX, IndexName}]).

-spec create_indexed_bucket_type(cluster(), binary(), index_name(),
                                 schema_name(), raw_schema()) -> ok.
create_indexed_bucket_type(Cluster, BucketType, IndexName, SchemaName,
                           RawSchema) ->
    Node = select_random(Cluster),
    Pid = rt:pbc(Node),
    ok = store_schema(Pid, SchemaName, RawSchema),
    ok = wait_for_schema(Cluster, SchemaName),
    ok = create_index(Cluster, IndexName, SchemaName),
    ok = create_bucket_type(Cluster, BucketType, [{?YZ_INDEX, IndexName}]).

-spec create_index(cluster(), index_name()) -> ok.
create_index(Cluster, Index) ->
    Node = select_random(Cluster),
    lager:info("Creating index ~s [~p]", [Index, Node]),
    rpc:call(Node, yz_index, create, [Index]),
    ok = wait_for_index(Cluster, Index).

-spec create_index(cluster(), index_name(), schema_name()) -> ok.
create_index(Cluster, Index, SchemaName) ->
    Node = select_random(Cluster),
    lager:info("Creating index ~s with schema ~s [~p]",
               [Index, SchemaName, Node]),
    rpc:call(Node, yz_index, create, [Index, SchemaName]),
    ok = wait_for_index(Cluster, Index).

-spec create_index(cluster(), index_name(), schema_name(), n()) -> ok.
create_index(Cluster, Index, SchemaName, NVal) ->
    Node = select_random(Cluster),
    lager:info("Creating index ~s with schema ~s and n_val: ~p [~p]",
               [Index, SchemaName, NVal, Node]),
    rpc:call(Node, yz_index, create, [Index, SchemaName, NVal]),
    ok = wait_for_index(Cluster, Index).

-spec create_index_http(cluster(), index_name()) -> ok.
create_index_http(Cluster, Index) ->
    Node = select_random(Cluster),
    HP = hd(host_entries(rt:connection_info([Node]))),
    create_index_http(Cluster, HP, Index).

-spec create_index_http(cluster(), {string(), portnum()}, index_name()) -> ok.
create_index_http(Cluster, HP, Index) ->
    Node = hd(Cluster),
    URL = index_url(HP, Index),
    Headers = [{"content-type", "application/json"}],
    lager:info("create_index ~s [~p]", [Index, Node]),
    {ok, Status, _, _} = http(put, URL, Headers, ?NO_BODY),
    ?assert(Status == "204" orelse Status == "202"),
    ok = wait_for_index(Cluster, Index),
    set_bucket_type_index(Cluster, Index),
    %% TODO: This may no longer be required, as set_bucket_type_index
    %% (above) will ultimately wait for the bucket type to be visible
    %% throughout the cluster
    wait_for_bucket_type(Cluster, Index).

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

-spec count_calls(cluster(), {atom(), atom(), non_neg_integer()}) -> ok.
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

-spec http(method(), string(), list(), binary()|[]) -> response().
http(Method, URL, Headers, Body) ->
    Opts = [],
    ibrowse:send_req(URL, Headers, Method, Body, Opts, ?IBROWSE_TIMEOUT).

-spec http(method(), string(), list(), binary()|[], list()|timeout())
          -> response().
http(Method, URL, Headers, Body, Opts) when is_list(Opts)  ->
    ibrowse:send_req(URL, Headers, Method, Body, Opts, ?IBROWSE_TIMEOUT);
http(Method, URL, Headers, Body, Timeout) when is_integer(Timeout) ->
    Opts = [],
    ibrowse:send_req(URL, Headers, Method, Body, Opts, Timeout).

-spec http(method(), string(), list(), binary()|[], list(), timeout())
          -> response().
http(Method, URL, Headers, Body, Opts, Timeout) when
      is_list(Opts) andalso is_integer(Timeout) ->
    ibrowse:send_req(URL, Headers, Method, Body, Opts, Timeout).

-spec http_put({host(), portnum()}, bucket(), binary()|string(), binary()) -> ok.
http_put(HP, Bucket, Key, Value) ->
    http_put(HP, Bucket, Key, "text/plain", Value).

-spec http_put({host(), portnum()}, bucket(), binary()|string(), string(),
               binary()) -> ok.
http_put({Host, Port}, {BType, BName}, Key, CT, Value) ->
    URL = ?FMT("http://~s:~s/types/~s/buckets/~s/keys/~s",
               [Host, integer_to_list(Port), BType, BName, Key]),
    Opts = [],
    Headers = [{"content-type", CT}],
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Value, Opts,
                                         ?IBROWSE_TIMEOUT),
    ok.

-spec schema_url({host(), portnum()}, schema_name()) -> string().
schema_url({Host,Port}, Name) ->
    ?FMT("http://~s:~B/search/schema/~s", [Host, Port, Name]).

-spec index_url({host(), portnum()}, index_name()) -> string().
index_url({Host,Port}, Index) ->
    ?FMT("http://~s:~B/search/index/~s", [Host, Port, Index]).
index_url({Host, Port}, Index, Timeout) ->
    ?FMT("http://~s:~B/search/index/~s?timeout=~B", [Host, Port, Index,
                                                     Timeout]).
-spec search_url({host(), portnum()}, index_name()) -> string().
search_url({Host, Port}, Index) ->
    search_url({Host, Port}, Index, "").

-spec search_url({host(), portnum()}, index_name(), string()) -> string().
search_url({Host, Port}, Index, Params) ->
    FmtStr = "http://~s:~B/search/query/~s",
    FmtParams = case Params of
                    "" -> "";
                    _ -> "?" ++ Params
                end,
    ?FMT(FmtStr ++ FmtParams, [Host, Port, Index]).

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


-spec write_data(cluster(), bucket(), non_neg_integer(), non_neg_integer()) ->
    [ok | {error, term()}].
write_data(Cluster, Bucket, Start, Count) ->
    ValueGenerator = fun(Key) -> {Key, "text/plain"} end,
    write_data(Cluster, Bucket, Start, Count, ValueGenerator).

-spec write_data(cluster(), bucket(), non_neg_integer(), non_neg_integer(), fun((non_neg_integer()) -> ok | term()))  ->
    [ok | {error, term()}].
write_data(Cluster, Bucket, Start, Count, ValueGenerator) ->
    Node = select_random(Cluster),
    rt:wait_for_service(Node, riak_kv),
    Pid = rt:pbc(Node),
    F = fun(N, Acc) ->
        Key = list_to_binary(integer_to_list(N)),
        {Value, ContentType} = ValueGenerator(Key),
        Obj = riakc_obj:new(Bucket, Key, Value, ContentType),
        %lager:info("Writing {~p, ~p} ...", [Key, Value]),
        try riakc_pb_socket:put(Pid, Obj) of
            ok ->
                [ok | Acc];
            Other ->
                [{error, Other} | Acc]
        catch
            What:Why ->
                [{error, {What, Why}} | Acc]
        end
    end,
    lager:info("Writing ~p objects to Riak...", [Count]),
    lists:foldl(F, [], lists:seq(Start, (Start + Count) - 1)).

%% @doc Load the BEAM for `Module' across the `Nodes'.  This allows
%% use of higher order functions, defined in a riak test module, on
%% the Riak node.
%%
%% @see yz_mapreduce:collect_results/2
-spec load_module(cluster(), module()) -> ok.
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

-spec random_binary(pos_integer()) -> binary().
random_binary(Length) when Length >= 1 ->
    Chars = "abcdefghijklmnopqrstuvwxyz1234567890",
    Value = lists:foldl(fun(_, Acc) ->
            [lists:nth(random:uniform(length(Chars)), Chars)] ++ Acc
        end, [], lists:seq(1, Length)),
    list_to_binary(Value).

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

-spec search_expect(node()|cluster(), index_name(), string(), string(),
                    non_neg_integer()) -> ok.
search_expect(NodeOrNodes, Index, Name, Term, Expect) ->
    search_expect(NodeOrNodes, yokozuna, Index, Name, Term, Expect).

-spec search_expect(node()|cluster(), search_type(), index_name(),
                    string(), string(), [string()], non_neg_integer()) -> ok.
search_expect(Nodes, solr, Index, Name0, Term0, Shards, Expect)
  when is_list(Shards), length(Shards) > 0, is_list(Nodes) ->
    Name = quote_unicode(Name0),
    Term = quote_unicode(Term0),
    Node = select_random(Nodes),
    {Host, Port} = solr_hp(Node, Nodes),
    URL = internal_solr_url(Host, Port, Index, Name, Term, Shards),
    lager:info("Run solr search ~s", [URL]),
    Opts = [{response_format, binary}],
    F = fun(_) ->
                {ok, "200", _, R} = ibrowse:send_req(URL, [], get, [], Opts,
                                                     ?IBROWSE_TIMEOUT),
                verify_count(Expect, R)
        end,
    wait_until(Nodes, F);
search_expect(Node, solr=Type, Index, Name, Term, Shards, Expect)
  when is_list(Shards), length(Shards) > 0 ->
    search_expect([Node], Type, Index, Name, Term, Shards, Expect).

-spec search_expect(node()|cluster(), search_type(), index_name(),
                    string(), string(), non_neg_integer()) -> ok.
search_expect(Nodes, solr=Type, Index, Name, Term, Expect) when is_list(Nodes) ->
    Node = select_random(Nodes),
    HP = solr_hp(Node, Nodes),

    %% F could actually be returned in a shared fun, but w/ so much arity,
    %% just using it twice makes sense.
    F = fun(_) ->
                {ok, "200", _, R} = search(Type, HP, Index, Name, Term),
                verify_count(Expect, R)
        end,

    wait_until(Nodes, F);
search_expect(Nodes, yokozuna=Type, Index, Name, Term, Expect)
  when is_list(Nodes) ->
    HP = hd(host_entries(rt:connection_info(Nodes))),

    F = fun(_) ->
                {ok, "200", _, R} = search(Type, HP, Index, Name, Term),
                verify_count(Expect, R)
        end,

    wait_until(Nodes, F);
search_expect(Node, Type, Index, Name, Term, Expect) ->
    search_expect([Node], Type, Index, Name, Term, Expect).

search(HP, Index, Name, Term) ->
    search(HP, Index, Name, Term, "").

search({Host, Port}, Index, Name, Term, Params) ->
    search(yokozuna, {Host, Port}, Index, Name, Term, Params);
search(Type, {Host, Port}, Index, Name, Term) when is_integer(Port) ->
    search(Type, {Host, integer_to_list(Port)}, Index, Name, Term);
search(Type, {Host, Port}, Index, Name, Term) ->
    search(Type, {Host, Port}, Index, Name, Term, "").
search(Type, {Host, Port}, Index, Name0, Term0, Params0) ->
    search(Type, {Host, Port}, Index, Name0, Term0, Params0, true).
search(Type, {Host, Port}, Index, Name0, Term0, Params0, Noisy) when is_integer(Port)->
    search(Type, {Host, integer_to_list(Port)}, Index, Name0, Term0, Params0, Noisy);
search(Type, {Host, Port}, Index, Name0, Term0, Params0, Noisy) ->
    Name = quote_unicode(Name0),
    Term = quote_unicode(Term0),
    Params = "q=" ++ Name ++ ":" ++ Term ++ "&wt=json&" ++
             urlencode_params(Params0),
    URL = case Type of
              solr ->
                  ?FMT("http://~s:~s/internal_solr/~s/select?~s",
                       [Host, Port, Index, Params]);
              yokozuna ->
                  search_url({Host, integer_portnum(Port)}, Index, Params)
          end,
    ?IF(Noisy, lager:info("Run search ~s", [URL])),
    Opts = [{response_format, binary}],
    ibrowse:send_req(URL, [], get, [], Opts, ?IBROWSE_TIMEOUT).

integer_portnum(Port) when is_integer(Port) ->
    Port;
integer_portnum(Port) when is_list(Port) ->
    list_to_integer(Port).

%% @doc Encodes the proplist argument as a URL query string. If the argument
%% is not a proplist, this function assumes that it is already a URL-encoded
%% string and returns it unmodified.
-spec urlencode_params([proplists:property()] | string()) -> string().
urlencode_params([{_Key, _Value}|_Rest] = Params) ->
    mochiweb_util:urlencode(Params);
urlencode_params(String) ->
    String.

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

really_remove_index(Cluster, {BucketType, Bucket}, Index, PBConn) ->
    lager:info("Removing index ~p", [Index]),
    Node = hd(Cluster),
    F = fun(_) ->
                Props = [{?YZ_INDEX, ?YZ_INDEX_TOMBSTONE}],
                %% Remove index from bucket props and delete it
                rpc:call(Node, riak_core_bucket, set_bucket, [{BucketType, Bucket}, Props]),
                remove_index(Node, BucketType),
                DelResp = riakc_pb_socket:delete_search_index(PBConn, Index),
                case DelResp of
                    ok -> true;
                    {error,<<"notfound">>} -> true;
                    _ ->
                        lager:info("Attempting to remove index ~p: Result was ~p", [Index, DelResp]),
                        false
                end
        end,
    yz_rt:wait_until(Cluster, F).

-spec set_bucket_type_index(cluster(), binary()) -> ok.
set_bucket_type_index(Cluster, BucketType) ->
    set_bucket_type_index(Cluster, BucketType, BucketType).

-spec set_bucket_type_index(cluster(), binary(), index_name()) -> ok.
set_bucket_type_index(Cluster, BucketType, Index) ->
    lager:info("Set bucket type ~s index to ~s ~p", [BucketType, Index, Cluster]),
    create_bucket_type(Cluster, BucketType, [{?YZ_INDEX, Index}]).

-spec set_bucket_type_index(cluster(), binary(), index_name(), non_neg_integer()) -> ok.
set_bucket_type_index(Cluster, BucketType, Index, NVal) ->
    lager:info("Set bucket type ~s index to ~s ~p", [BucketType, Index, Cluster]),
    create_bucket_type(Cluster, BucketType, [{?YZ_INDEX, Index},{n_val,NVal}]).

%%TODO: consolidate this with create_index_bucket_type if possible
-spec create_indexed_bucket(pid(),
                            cluster(),
                            {binary(), binary()},
                            index_name()) -> ok.
create_indexed_bucket(PBConn, Cluster, Bucket, Index) ->
    create_indexed_bucket(PBConn, Cluster, Bucket, Index, 1).

-spec create_indexed_bucket(pid(),
                            cluster(),
                            {binary(), binary()},
                            index_name(),
                            pos_integer()) -> ok.
create_indexed_bucket(PBConn, Cluster, {BType, Bucket}, Index, NVal) ->
    lager:info("Creating index ~p", [Index]),
    ok = riakc_pb_socket:create_search_index(PBConn, Index, <<>>, [{n_val, NVal}]),
    lager:info("Binding index ~p to bucket ~p", [Index, {BType, Bucket}]),
    ok = set_bucket_type_index(Cluster, BType, Index, NVal).

-spec solr_http({node(), orddict:orddict()}) -> {host(), portnum()}.
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
-spec wait_for_all_trees(cluster()) -> ok.
wait_for_all_trees(Cluster) ->
    F = fun(Node) ->
                lager:info("Check if all trees built for node ~p", [Node]),
                Info = rpc:call(Node, yz_kv, compute_tree_info, []),
                NotBuilt = [X || {_,undefined}=X <- Info],
                NotBuilt == []
        end,
    wait_until(Cluster, F),
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

-spec wait_for_full_exchange_round(cluster()) -> ok.
wait_for_full_exchange_round(Cluster) ->
    wait_for_full_exchange_round(Cluster, erlang:now()).

%% @doc Wait for a full exchange round since `Timestamp'.  This means
%% that all `{Idx,N}' for all partitions must have exchanged after
%% `Timestamp'.
-spec wait_for_full_exchange_round(cluster(), timestamp()) -> ok.
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
    wait_until(Cluster, AllExchanged),
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
    wait_until(Cluster, F),
    ok.

-spec verify_count(non_neg_integer(), json_string()) -> boolean().
verify_count(Expected, Resp) ->
    Count = get_count(Resp),
    lager:info("E: ~p, A: ~p", [Expected, Count]),
    Expected =:= Count.

-spec verify_num_match(cluster(), index_name(), non_neg_integer()) -> ok.
verify_num_match(Cluster, Index, Num) ->
    verify_num_match(yokozuna, Cluster, Index, Num).
verify_num_match(Type, Cluster, Index, Num) ->
    if Type =:= solr ->
            Shards = [{N, node_solr_port(N)} || N <- Cluster],
            search_expect(Cluster, Type, Index, "*", "*", Shards,
                          Num);
       true ->
            search_expect(Cluster, Type, Index, "*", "*", Num)
    end.

-spec wait_for_index(cluster(), index_name()) -> ok.
wait_for_index(Cluster, Index) ->
    IsIndexUp =
        fun(Node) ->
                lager:info("Waiting for index ~s to be available on node ~p",
                           [Index, Node]),
                rpc:call(Node, yz_index, exists, [Index])
        end,
    wait_until(Cluster, IsIndexUp),
    ok.

-spec wait_for_solr(cluster()) -> ok.
wait_for_solr(Cluster) ->
    IsSolrUp =
        fun(Node) ->
            lager:info("Waiting for Solr to come up on node ~p", [Node]),
            rpc:call(Node, yz_solr, is_up, [])
        end,
    wait_until(Cluster, IsSolrUp),
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
-spec wait_until(cluster(), fun((node()) -> boolean())) -> ok.
wait_until(Nodes, F) ->
    [?assertEqual(ok, rt:wait_until(Node, F)) || Node <- Nodes],
    ok.

-spec node_solr_port(node()) -> port().
node_solr_port(Node) ->
    {ok, P} = riak_core_util:safe_rpc(Node, application, get_env,
                                      [yokozuna, solr_port]),
    P.

-spec internal_solr_url(host(), portnum(), index_name()) -> string().
internal_solr_url(Host, Port, Index) ->
    ?FMT("http://~s:~B/internal_solr/~s", [Host, Port, Index]).

-spec internal_solr_url(host(), portnum(), index_name(), [{host(), portnum()}])
                       -> string().
internal_solr_url(Host, Port, Index, Shards) ->
    internal_solr_url(Host, Port, Index, <<"*">>, <<"*">>, Shards).

-spec internal_solr_url(host(), portnum(), index_name(), binary()|string(),
                        binary()|string(), [{host(), portnum()}]) -> string().
internal_solr_url(Host, Port, Index, Name, Term, Shards) ->
    Ss = [internal_solr_url(Host, ShardPort, Index)
          || {_, ShardPort} <- Shards],
    ?FMT("http://~s:~B/internal_solr/~s/select?wt=json&q=~s:~s&shards=~s",
         [Host, Port, Index, Name, Term, string:join(Ss, ",")]).

entropy_data_url({Host, Port}, Index, Params) ->
    ?FMT("http://~s:~B/internal_solr/~s/entropy_data?~s",
         [Host, Port, Index, mochiweb_util:urlencode(Params)]).

-spec merge_config(proplist(), proplist()) -> proplist().
merge_config(Change, Base) ->
    lists:ukeymerge(1, lists:keysort(1, Change), lists:keysort(1, Base)).

-spec write_objs(cluster(), bucket()) -> ok.
write_objs(Cluster, Bucket) ->
    write_objs(Cluster, Bucket, 1000).

-spec write_objs([node()], bucket(), pos_integer()) -> ok.
write_objs(Cluster, Bucket, Count) ->
    write_objs(Cluster, Bucket, 1, Count).

-spec write_objs([node()], bucket(), pos_integer(), pos_integer()) -> ok.
write_objs(Cluster, Bucket, StartIndex, Count) ->
    lager:info("Writing ~p objects", [Count]),
    lists:foreach(write_obj(Cluster, Bucket), lists:seq(StartIndex,
                                                        StartIndex + Count - 1)).

write_objs_parallel(Cluster, Bucket, Count, NumWorkers) ->
    write_objs_parallel(Cluster, Bucket, 1, Count, NumWorkers).

write_objs_parallel(Cluster, Bucket, StartIndex, Count, NumWorkers) ->
    lager:info("Parallel writing ~p objects from ~p using ~p workers",
               [Count, StartIndex, NumWorkers]),
    ObjsPerWorker = Count div NumWorkers,
    rt:pmap(
      fun(WorkerIdx) ->
              WorkerStartIndex = StartIndex + (WorkerIdx - 1) * ObjsPerWorker,
              write_objs(Cluster, Bucket, WorkerStartIndex, ObjsPerWorker)
      end,
      lists:seq(1, NumWorkers)).

-spec write_obj(cluster(), bucket()) -> fun().
write_obj(Cluster, Bucket) ->
    fun(N) ->
            PL = [{name_s,<<"yokozuna">>}, {num_i,N}],
            Key = list_to_binary(io_lib:format("key_~B", [N])),
            Body = mochijson2:encode(PL),
            HP = select_random(host_entries(rt:connection_info(Cluster))),
            CT = "application/json",
            lager:info("Writing object with bkey ~p [~p]", [{Bucket, Key}, HP]),
            http_put(HP, Bucket, Key, CT, Body)
    end.

-spec commit(cluster(), index_name()) -> ok.
commit(Nodes, Index) ->
    %% Wait for yokozuna index to trigger, then force a commit
    timer:sleep(?SOFTCOMMIT),
    lager:info("Commit search writes to ~s at softcommit (default) ~p",
               [Index, ?SOFTCOMMIT]),
    rpc:multicall(Nodes, yz_solr, commit, [Index]),
    ok.

-spec drain_solrqs(node() | cluster()) -> ok.
drain_solrqs(Cluster) when is_list(Cluster) ->
    [drain_solrqs(Node) || Node <- Cluster];
drain_solrqs(Node) ->
    rpc:call(Node, yz_solrq_drain_mgr, drain, []),
    ok.

-spec load_intercept_code(node() | cluster()) -> ok.
load_intercept_code(Cluster) when is_list(Cluster) ->
    [load_intercept_code(Node) || Node <- Cluster];
load_intercept_code(Node) ->
    CodePath = filename:join([rt_config:get(yz_dir),
                              "riak_test",
                              "intercepts",
                              "*.erl"]),
    rt_intercept:load_code(Node, [CodePath]).

rolling_upgrade(Cluster, Version, UpgradeConfig, WaitForServices) ->
    rolling_upgrade(Cluster, Version, UpgradeConfig, WaitForServices, fun rt:no_op/1).

-spec rolling_upgrade(cluster() | node(),
                      current | previous | legacy,
                      UpgradeConfig :: props(),
                      WaitForServices :: [atom()]) -> ok.
rolling_upgrade(Cluster, Version, UpgradeConfig, WaitForServices, UpgradeCallabck)
  when is_list(Cluster) ->
    lager:info("Perform rolling upgrade on cluster ~p", [Cluster]),
    [rolling_upgrade(Node, Version, UpgradeConfig, WaitForServices, UpgradeCallabck)
     || Node <- Cluster],
    ok;
rolling_upgrade(Node, Version, UpgradeConfig, WaitForServices, UpgradeCallabck) ->
    rt:upgrade(Node, Version, UpgradeConfig, UpgradeCallabck),
    [rt:wait_for_service(Node, Service) || Service <- WaitForServices],
    ok.

-spec setup_drain_intercepts(cluster()) -> ok.
setup_drain_intercepts(Cluster) ->
    [load_intercept_code(Node) || Node <- Cluster],
    [rt_intercept:add(
       N,
       {yz_solrq_drain_mgr, [{{drain, 1}, delay_drain}]}
      ) || N <- Cluster],
    ok.

-spec expire_aae_trees(cluster()) -> [ok].
expire_aae_trees(Cluster) ->
    lager:info("Expiring YZ AAE trees across cluster"),
    [ok = rpc:call(Node, yz_entropy_mgr, expire_trees, []) || Node <- Cluster].

-spec clear_aae_trees(cluster()) -> [ok].
clear_aae_trees(Cluster) ->
    lager:info("Clearing YZ AAE trees across cluster"),
    [ok = rpc:call(Node, yz_entropy_mgr, clear_trees, []) || Node <- Cluster].

-spec expire_kv_trees(cluster()) -> [ok].
expire_kv_trees(Cluster) ->
    lager:info("Expiring KV AAE trees across cluster"),
    [ok = rpc:call(Node, riak_kv_entropy_manager, expire_trees, []) || Node <- Cluster].

-spec clear_kv_trees(cluster()) -> [ok].
clear_kv_trees(Cluster) ->
    lager:info("Clearing KV AAE trees across cluster"),
    [ok = rpc:call(Node, riak_kv_entropy_manager, clear_trees, []) || Node <- Cluster].

-spec set_index(cluster(), index_name(), solrq_batch_min(), solrq_batch_max(),
                solrq_batch_flush_interval()) -> {[any()],[atom()]}.
set_index(Cluster, Index, Min, Max, DelayMsMax) ->
    rpc:multicall(Cluster, yz_solrq, set_index, [Index, Min, Max, DelayMsMax]).

-spec set_hwm(cluster(), pos_integer()) -> {[any()],[atom()]}.
set_hwm(Cluster, Hwm) ->
    rpc:multicall(Cluster, yz_solrq, set_hwm, [Hwm]).

-spec set_purge_strategy(cluster(), purge_strategy()) -> {[any()],[atom()]}.
set_purge_strategy(Cluster, PurgeStrategy) ->
    rpc:multicall(Cluster, yz_solrq, set_purge_strategy, [PurgeStrategy]).

-spec wait_until_fuses_blown(node() | cluster(), p(), [index_name()]) ->
                                    ok | [ok].
wait_until_fuses_blown(Cluster, Partition, Indices) when is_list(Cluster) ->
    [wait_until_fuses_blown(Node, Partition, Indices) || Node <- Cluster];
wait_until_fuses_blown(Node, Partition, Indices) ->
    F = fun({Index, IndexQ}) ->
                lager:info("Waiting for fuse to blow for index ~p", [{Index, IndexQ}]),
                proplists:get_value(fuse_blown, IndexQ)
        end,
    check_fuse_status(Node, Partition, Indices, F).

-spec wait_until_fuses_reset(node() | cluster(), p(), [index_name()]) ->
                                    ok | [ok].
wait_until_fuses_reset(Cluster, Partition, Indices) when is_list(Cluster) ->
    [wait_until_fuses_reset(Node, Partition, Indices) || Node <- Cluster];
wait_until_fuses_reset(Node, Partition, Indices) ->
    F = fun({Index, IndexQ}) ->
                lager:info("Waiting for fuse to reset for index ~p", [{Index, IndexQ}]),
                not proplists:get_value(fuse_blown, IndexQ)
        end,
    check_fuse_status(Node, Partition, Indices, F).

%% @private
check_fuse_status(Node, Partition, Indices, FuseCheckFunction) ->
    SolrQNames =
        [{Index, yz_solrq:worker_regname(Index, Partition)} ||
            Index <- Indices],
    F = fun(N) ->
                Solrqs = rpc:call(N, yz_solrq, status, []),
                IndexQs = [
                           begin
                               Solrq = proplists:get_value(SolrqName, Solrqs),
                               {Index, Solrq}
                           end ||
                              {Index, SolrqName} <- SolrQNames],
                MatchingIndexQs = lists:filter(
                                    FuseCheckFunction,
                                    IndexQs
                                   ),
                MatchingIndices = [Index || {Index, _IndexQ} <- MatchingIndexQs],
                sets:is_subset(sets:from_list(Indices), sets:from_list(MatchingIndices))
        end,
    wait_until([Node], F).

-spec intercept_index_batch(node() | cluster(), module()) -> ok | [ok].
intercept_index_batch(Cluster, Intercept) ->
    add_intercepts(
      Cluster,
      yz_solr, [{{index_batch, 2}, Intercept}]).

-spec add_intercepts(node() | cluster(), module(), intercepts()) -> ok | [ok].
add_intercepts(Cluster, Module, Intercepts) when is_list(Cluster) ->
    [add_intercepts(Node, Module, Intercepts) || Node <- Cluster];
add_intercepts(Node, Module, Intercepts) ->
    rt_intercept:add(
      Node,
      {Module, Intercepts}).

-spec set_yz_aae_mode(node() | cluster(), automatic | manual) -> ok | [ok].
set_yz_aae_mode(Cluster, Mode) when is_list(Cluster) ->
    [set_yz_aae_mode(Node, Mode) || Node <- Cluster];
set_yz_aae_mode(Node, Mode) ->
    rpc:call(Node, yz_entropy_mgr, set_mode, [Mode]).

%% @doc Generate `SeqMax' keys. Yokozuna supports only UTF-8 compatible keys.
-spec gen_keys(pos_integer()) -> [binary()].
gen_keys(SeqMax) ->
    [<<N:64/integer>> || N <- lists:seq(1, SeqMax),
                         not lists:any(
                               fun(E) -> E > 127 end,
                               binary_to_list(<<N:64/integer>>))].

-spec check_stat_values(
        proplists:proplist() | {error, Reason :: term()},
        [{StatName::atom(), LHS::term(), Comparator::atom(), RHS::term()}])
                       -> boolean().
check_stat_values(Stats, Pairs) ->
    lager:debug("STATS: ~p", [Stats]),
    lager:info("Pairs: ~p", [Pairs]),
    StillWaiting = [S || S = {_, Value, Cmp, Arg} <- Pairs,
                         not (erlang:Cmp(Value, Arg))],
    case StillWaiting of
        [] ->
            true;
        _ ->
            lager:info("Waiting for stats: ~p", [StillWaiting]),
            false
    end.

-spec solr_hp(node(), cluster()) -> {host(), portnum()}.
solr_hp(Node, Cluster) ->
    CI = connection_info(Cluster),
    solr_http(proplists:get_value(Node, CI)).

-spec reset_stats(cluster()) -> [[{list(), ok | {error, any()}}]].
reset_stats(Cluster) ->
    [rpc:call(Node, yz_stat, reset, []) || Node <- Cluster].

%% returns a dict of the form Solrq -> [BKey] that partitions BKeys
%% by the solrqs they hash into.
%% i.e., Union([BKey]) = BKeys and Intersection([BKey]) = []
%% and for each BKey in BKeys, BKey is in dict(Solrq) iff BKey
%% hashes to Solrq.
-spec find_representatives(index_name(), [bkey()], non_neg_integer()) -> dict().
find_representatives(Index, BKeys, RingSize) ->
    lists:foldl(
        fun({Solrq, BKey}, Accum) ->
            dict:append(Solrq, BKey, Accum)
        end,
        dict:new(),
        [{get_solrq(Index, BKey, RingSize), BKey} || BKey <- BKeys]).

%% returns the solrq proc name that the BKey would hash into
-spec get_solrq(index_name(), {bucket(), key()}, non_neg_integer()) -> atom().
get_solrq(Index, BucketKey, RingSize) ->
    Hash = chash:key_of(BucketKey),
    Partition = riak_core_ring_util:partition_id_to_hash(
        riak_core_ring_util:hash_to_partition_id(Hash, RingSize), RingSize),
    yz_solrq:worker_regname(Index, Partition).
