%% @doc Test the index adminstration API in various ways.
-module(yz_pb).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_yokozuna_pb.hrl").
-include("yokozuna.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(NO_HEADERS, []).
-define(NO_BODY, <<>>).
-define(CFG, [{yokozuna, [{enabled, true}]}]).

-define(SCHEMA_CONTENT, <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" />
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_node\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
</fields>
<uniqueKey>_yz_id</uniqueKey>
<types>
    <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
</types>
</schema>">>).

confirm() ->
    YZBenchDir = rt_config:get(yz_dir) ++ "/misc/bench",
    code:add_path(filename:join([YZBenchDir, "ebin"])),
    random:seed(now()),
    Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    confirm_admin_schema(Cluster),
    confirm_admin_index(Cluster),
    confirm_basic_search(Cluster),
    confirm_encoded_search(Cluster),
    confirm_multivalued_field(Cluster),
    pass.

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

host_entries(ClusterConnInfo) ->
    [proplists:get_value(http, I) || {_,I} <- ClusterConnInfo].

schema_url({Host,Port}, Name) ->
    ?FMT("http://~s:~B/yz/schema/~s", [Host, Port, Name]).

index_url({Host,Port}, Index) ->
    ?FMT("http://~s:~B/yz/index/~s", [Host, Port, Index]).

bucket_url({Host,Port}, Bucket, Key) ->
    ?FMT("http://~s:~B/buckets/~s/keys/~s", [Host, Port, Bucket, Key]).

http(Method, URL, Headers, Body) ->
    Opts = [],
    ibrowse:send_req(URL, Headers, Method, Body, Opts).

create_index(Cluster, Index, Bucket) ->
    Node = select_random(Cluster),
    [{Host, Port}] = host_entries(rt:connection_info([Node])),
    lager:info("create_index ~s for bucket ~s [~p]", [Index, Bucket, {Host, Port}]),
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    %% set index in props with the same name as the bucket
    ?assertEqual(ok, riakc_pb_socket:create_search_index(Pid, Index)),
    % Add the index to the bucket props
    yz_rt:set_index(Node, Index, Bucket),
    yz_rt:wait_for_index(Cluster, Index),
    %% Check that the index exists
    {ok, IndexData} = riakc_pb_socket:get_search_index(Pid, Index),
    ?assertEqual([{index,Index},{schema,?YZ_DEFAULT_SCHEMA_NAME}], IndexData),
    riakc_pb_socket:stop(Pid),
    ok.

store_and_search(Cluster, Bucket, Key, Body, Search, Params) ->
    store_and_search(Cluster, Bucket, Key, Body, "text/plain", Search, Params).

store_and_search(Cluster, Bucket, Key, Body, CT, Search, Params) ->
    {Host, Port} = select_random(host_entries(rt:connection_info(Cluster))),
    URL = bucket_url({Host, Port}, Bucket, Key),
    lager:info("Storing to bucket ~s", [URL]),
    %% populate a value
    {ok, "204", _, _} = ibrowse:send_req(URL, [{"Content-Type", CT}], put, Body),
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    F = fun(_) ->
        lager:info("Search for ~s [~p:~p]", [Search, Host, Port]),
        {ok,{search_results,R,Score,Found}} =
            riakc_pb_socket:search(Pid, Bucket, Search, Params),
        case Found of
            1 ->
                [{Bucket,Results}] = R,
                KeyCheck = (Key == binary_to_list(proplists:get_value(<<"_yz_rk">>, Results))),
                ScoreCheck = (Score =/= 0.0),
                KeyCheck and ScoreCheck;
            0 ->
                false
        end
    end,
    yz_rt:wait_until(Cluster, F),
    riakc_pb_socket:stop(Pid),
    ok.

confirm_admin_schema(Cluster) ->
    Name = <<"my_schema">>,
    Node = select_random(Cluster),
    {Host, Port} = yz_rt:riak_pb(hd(rt:connection_info([Node]))),
    lager:info("confirm_admin_schema ~s [~p]", [Name, {Host, Port}]),
    {ok, Pid} = riakc_pb_socket:start_link(Host, Port),
    yz_rt:store_schema(Pid, Name, ?SCHEMA_CONTENT),
    yz_rt:wait_for_schema(Cluster, Name, ?SCHEMA_CONTENT),
    MissingMessage = riakc_pb_socket:get_search_schema(Pid,<<"not_here">>),
    ?assertEqual({error,<<"notfound">>}, MissingMessage),
    riakc_pb_socket:stop(Pid),
    ok.

confirm_admin_index(Cluster) ->
    Index = <<"index">>,
    create_index(Cluster, Index, Index),
    Node = select_random(Cluster),
    [{Host, Port}] = host_entries(rt:connection_info([Node])),
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    F = fun(_) ->
        %% Remove index from bucket props and delete it
        yz_rt:set_index(Node, Index, <<>>),
        DelResp = riakc_pb_socket:delete_search_index(Pid, Index),
        case DelResp of
            ok -> true;
            {error,<<"notfound">>} -> true
        end
    end,
    yz_rt:wait_until(Cluster, F),
    riakc_pb_socket:stop(Pid),
    ok.

confirm_basic_search(Cluster) ->
    Bucket = <<"basic">>,
    create_index(Cluster, Bucket, Bucket),
    lager:info("confirm_basic_search ~s", [Bucket]),
    Body = "herp derp",
    Params = [{sort, <<"score desc">>}, {fl, ["*","score"]}],
    store_and_search(Cluster, Bucket, "test", Body, <<"text:herp">>, Params).

confirm_encoded_search(Cluster) ->
    Bucket = <<"encoded">>,
    create_index(Cluster, Bucket, Bucket),
    lager:info("confirm_encoded_search ~s", [Bucket]),
    Body = "א בְּרֵאשִׁית, בָּרָא אֱלֹהִים, אֵת הַשָּׁמַיִם, וְאֵת הָאָרֶץ",
    Params = [{sort, <<"score desc">>}, {fl, ["_yz_rk"]}],
    store_and_search(Cluster, Bucket, "וְאֵת", Body, <<"text:בָּרָא">>, Params).

confirm_multivalued_field(Cluster) ->
    Bucket = <<"basic">>,
    lager:info("cofirm multiValued=true fields decode properly"),
    create_index(Cluster, Bucket, Bucket),
    Body = <<"{\"name_ss\":\"turner\", \"name_ss\":\"hooch\"}">>,
    Params = [],
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    URL = bucket_url(HP, Bucket, "multivalued"),
    lager:info("Storing to bucket ~s", [URL]),
    {Host, Port} = HP,
    %% populate a value
    {ok, "204", _, _} = ibrowse:send_req(URL, [{"Content-Type", "application/json"}], put, Body),
    %% Sleep for soft commit
    timer:sleep(1100),
    Search = <<"name_ss:turner">>,
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    {ok,{search_results,[{Bucket,Fields}],_Score,_Found}} =
            riakc_pb_socket:search(Pid, Bucket, Search, Params),
    ?assert(lists:member({<<"name_ss">>,<<"turner">>}, Fields)),
    ?assert(lists:member({<<"name_ss">>,<<"hooch">>}, Fields)).
