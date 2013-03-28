%% @doc Test the index adminstration API in various ways.
-module(yz_languages).
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
   <field name=\"text_t\" type=\"text_general\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>
   <field name=\"text_ja\" type=\"text_ja\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>
   <dynamicField name=\"*_s\"  type=\"string\"  indexed=\"true\"  stored=\"true\" />
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
    <fieldType name=\"text_ja\" class=\"solr.TextField\" positionIncrementGap=\"100\" autoGeneratePhraseQueries=\"false\">
      <analyzer>
        <tokenizer class=\"solr.JapaneseTokenizerFactory\" mode=\"search\"/>
        <filter class=\"solr.JapaneseBaseFormFilterFactory\"/>
        <filter class=\"solr.JapanesePartOfSpeechStopFilterFactory\" tags=\"lang/stoptags_ja.txt\" enablePositionIncrements=\"true\"/>
        <filter class=\"solr.CJKWidthFilterFactory\"/>
        <filter class=\"solr.StopFilterFactory\" ignoreCase=\"true\" words=\"lang/stopwords_ja.txt\" enablePositionIncrements=\"true\" />
        <filter class=\"solr.JapaneseKatakanaStemFilterFactory\" minimumLength=\"4\"/>
        <filter class=\"solr.LowerCaseFilterFactory\"/>
      </analyzer>
    </fieldType>
</types>
</schema>">>).

confirm() ->
    YZBenchDir = rt:get_os_env("YZ_BENCH_DIR"),
    code:add_path(filename:join([YZBenchDir, "ebin"])),
    random:seed(now()),
    Cluster = prepare_cluster(4),
    confirm_create_schema(Cluster, <<"language_schema">>, ?TEST_SCHEMA),
    confirm_body_search_encoding(Cluster),
    confirm_language_field_type(Cluster),
    confirm_tag_encoding(Cluster),
    pass.

prepare_cluster(NumNodes) ->
    %% Note: may need to use below call b/c of diff between
    %% deploy_nodes/1 & /2
    %%
    % Nodes = rt:deploy_nodes(NumNodes, ?CFG),
    Nodes = rt:deploy_nodes(NumNodes),
    Cluster = join(Nodes),
    wait_for_joins(Cluster),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    Cluster.

join(Nodes) ->
    [NodeA|Others] = Nodes,
    [rt:join(Node, NodeA) || Node <- Others],
    Nodes.

wait_for_joins(Cluster) ->
    lager:info("Waiting for ownership handoff to finish"),
    rt:wait_until_nodes_ready(Cluster),
    rt:wait_until_no_pending_changes(Cluster).

%% @doc Confirm a custom schema may be added.
confirm_create_schema(Cluster, Name, RawSchema) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_create_schema ~s [~p]", [Name, HP]),
    URL = schema_url(HP, Name),
    Headers = [{"content-type", "application/xml"}],
    {ok, Status, _, _} = http(put, URL, Headers, RawSchema),
    ?assertEqual("204", Status).

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

search_url({Host,Port}, Bucket, Term) ->
    ?FMT("http://~s:~B/search/~s?wt=json&omitHeader=true&q=~s", [Host, Port, Bucket, Term]).

bucket_url({Host,Port}, Bucket, Key) ->
    ?FMT("http://~s:~B/riak/~s/~s", [Host, Port, Bucket, Key]).

http(Method, URL, Headers, Body) ->
    Opts = [],
    ibrowse:send_req(URL, Headers, Method, Body, Opts).

create_index(HP, Index) ->
    lager:info("create_index ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    Headers = [{"content-type", "application/json"}],
    {ok, Status, _, _} = http(put, URL, Headers, <<"{\"schema\":\"language_schema\"}">>),
    % TODO: shouldn't need this
    timer:sleep(4000),
    ?assertEqual("204", Status).

search(HP, Index, Term) ->
    URL = search_url(HP, Index, Term),
    lager:info("Run search ~s", [URL]),
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, [], get, [], Opts) of
        {ok, "200", _, Resp} ->
            lager:info("Search resp ~p", [Resp]),
            Resp;
        Other ->
            {bad_response, Other}
    end.

verify_count(Expected, Resp) ->
    Struct = mochijson2:decode(Resp),
    NumFound = yz_driver:get_path(Struct, [<<"response">>, <<"numFound">>]),
    Expected == NumFound.

store_and_search(Cluster, Bucket, CT, Body, Search) ->
    Headers = [{"Content-Type", CT}],
    store_and_search(Cluster, Bucket, Headers, CT, Body, Search).

store_and_search(Cluster, Bucket, Headers, CT, Body, Search) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    create_index(HP, Bucket),
    URL = bucket_url(HP, Bucket, "test"),
    lager:info("Storing to bucket ~s", [URL]),
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body),
    %% Sleep for soft commit
    timer:sleep(1000),
    {ok, "200", _, Body} = ibrowse:send_req(URL, [{"accept", CT}], get, []),
    lager:info("Verify values are indexed"),
    R1 = search(HP, Bucket, Search),
    verify_count(1, R1),
    ok.

confirm_body_search_encoding(Cluster) ->
    Bucket = "test_iso_8859_8",
    lager:info("confirm_iso_8859_8 ~s", [Bucket]),
    Body = "א בְּרֵאשִׁית, בָּרָא אֱלֹהִים, אֵת הַשָּׁמַיִם, וְאֵת הָאָרֶץ",
    store_and_search(Cluster, Bucket, "text/plain", Body, "text:בָּרָא").

confirm_language_field_type(Cluster) ->
    Bucket = "test_shift_jis",
    lager:info("confirm_shift_jis ~s", [Bucket]),
    Body = "{\"text_ja\" : \"私はハイビスカスを食べるのが 大好き\"}",
    store_and_search(Cluster, Bucket, "application/json", Body, "text_ja:大好き").

confirm_tag_encoding(Cluster) ->
    Bucket = "test_iso_8859_6",
    lager:info("confirm_iso_8859_6 ~s", [Bucket]),
    Body = "أردت أن أقرأ كتابا عن تاريخ المرأة في فرنسا",
    Headers = [{"Content-Type", "text/plain"},
               {"x-riak-meta-yz-tags", "x-riak-meta-arabic_s"},
               {"x-riak-meta-arabic_s", "أقرأ"}],
    store_and_search(Cluster, Bucket, Headers, "text/plain", Body, "arabic_s:أقرأ").
