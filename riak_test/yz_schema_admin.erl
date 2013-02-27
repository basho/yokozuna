%% @doc Test the index schema API in various ways.
-module(yz_schema_admin).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(NO_HEADERS, []).
-define(NO_BODY, <<>>).
-define(TEST_SCHEMA,
        <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"id\" type=\"string\" indexed=\"true\" stored=\"true\" required=\"true\" />
   <field name=\"_yz_ed\" type=\"string\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_pn\" type=\"string\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_fpn\" type=\"string\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_vtag\" type=\"string\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_node\" type=\"string\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rk\" type=\"string\" indexed=\"true\" stored=\"true\"/>
   <field name=\"text\" type=\"text_general\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>
</fields>

 <uniqueKey>id</uniqueKey>

<types>
    <fieldType name=\"string\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"text_general\" class=\"solr.TextField\" positionIncrementGap=\"100\">
      <analyzer type=\"index\">
        <tokenizer class=\"solr.StandardTokenizerFactory\"/>
        <filter class=\"solr.StopFilterFactory\" ignoreCase=\"true\" words=\"stopwords.txt\" enablePositionIncrements=\"true\" />
        <!-- in this example, we will only use synonyms at query time
        <filter class=\"solr.SynonymFilterFactory\" synonyms=\"index_synonyms.txt\" ignoreCase=\"true\" expand=\"false\"/>
        -->
        <filter class=\"solr.LowerCaseFilterFactory\"/>
      </analyzer>
      <analyzer type=\"query\">
        <tokenizer class=\"solr.StandardTokenizerFactory\"/>
        <filter class=\"solr.StopFilterFactory\" ignoreCase=\"true\" words=\"stopwords.txt\" enablePositionIncrements=\"true\" />
        <filter class=\"solr.SynonymFilterFactory\" synonyms=\"synonyms.txt\" ignoreCase=\"true\" expand=\"true\"/>
        <filter class=\"solr.LowerCaseFilterFactory\"/>
      </analyzer>
    </fieldType>
</types>
</schema>">>).

confirm() ->
    Cluster = prepare_cluster(4),
    confirm_create_schema(Cluster, <<"test_schema">>, ?TEST_SCHEMA),
    confirm_get_schema(Cluster, <<"test_schema">>, ?TEST_SCHEMA),
    confirm_not_found(Cluster, <<"not_a_schema">>),
    confirm_bad_ct(Cluster, <<"bad_ct">>, ?TEST_SCHEMA),
    pass.

%% @doc Confirm a custom schema may be added.
confirm_create_schema(Cluster, Name, RawSchema) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_create_schema ~s [~p]", [Name, HP]),
    URL = schema_url(HP, Name),
    Headers = [{"content-type", "application/xml"}],
    {ok, Status, _, _} = http(put, URL, Headers, RawSchema),
    ?assertEqual("204", Status).

%% @doc Confirm the schema stored under `Name' matches `RawSchema'.
confirm_get_schema(Cluster, Name, RawSchema) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_get_schema ~s [~p]", [Name, HP]),
    URL = schema_url(HP, Name),
    {ok, Status, Headers, Body} = http(get, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("200", Status),
    ?assertEqual("application/xml", ct(Headers)),
    ?assertEqual(RawSchema, Body).

%% @doc Confirm 404 for an unknown schema.
confirm_not_found(Cluster, Name) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_not_found ~s [~p]", [Name, HP]),
    URL = schema_url(HP, Name),
    {ok, Status, _, _} = http(get, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("404", Status).

%% @doc Confirm a 415 if an incorrect content-type is given.
confirm_bad_ct(Cluster, Name, RawSchema) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_bad_ct ~s [~p]", [Name, HP]),
    URL = schema_url(HP, Name),
    Headers = [{"content-type", "application/json"}],
    {ok, Status, _, Body} = http(put, URL, Headers, RawSchema),
    ?assertEqual("415", Status),
    ?assertEqual(<<>>, Body).

%%%===================================================================
%%% Helpers
%%%===================================================================

ct(Headers) ->
    Headers2 = [{string:to_lower(Key), Value} || {Key, Value} <- Headers],
    proplists:get_value("content-type", Headers2).

host_entries(ClusterConnInfo) ->
    [proplists:get_value(http, I) || {_,I} <- ClusterConnInfo].

http(Method, URL, Headers, Body) ->
    Opts = [{response_format, binary}],
    ibrowse:send_req(URL, Headers, Method, Body, Opts).

schema_url({Host,Port}, Name) ->
    ?FMT("http://~s:~B/yz/schema/~s", [Host, Port, Name]).

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
