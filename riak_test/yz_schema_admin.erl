%% @doc Test the index schema API in various ways.
-module(yz_schema_admin).
-compile(export_all).
-import(yz_rt, [host_entries/1, select_random/1]).
-include_lib("eunit/include/eunit.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(NO_HEADERS, []).
-define(NO_BODY, <<>>).
-define(CFG, [{yokozuna, [{enabled, true}]}]).
-define(TEST_SCHEMA,
        <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" />
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\"/>
   <field name=\"_yz_node\" type=\"_yz_str\" indexed=\"true\"/>
   <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"text\" type=\"text_general\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>
</fields>

 <uniqueKey>_yz_id</uniqueKey>

<types>
    <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"text_general\" class=\"solr.TextField\" positionIncrementGap=\"100\">
      <analyzer type=\"index\">
        <tokenizer class=\"solr.StandardTokenizerFactory\"/>
        <filter class=\"solr.StopFilterFactory\" ignoreCase=\"true\" words=\"stopwords.txt\" enablePositionIncrements=\"true\" />
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

-define(TRUNCATED_SCHEMA,
        <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" />
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"tru">>).

-define(MISSING_YZ_FIELDS_SCHEMA,
        <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"text\" type=\"text_general\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>
</fields>

 <uniqueKey>_yz_id</uniqueKey>

<types>
    <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"text_general\" class=\"solr.TextField\" positionIncrementGap=\"100\">
      <analyzer type=\"index\">
        <tokenizer class=\"solr.StandardTokenizerFactory\"/>
        <filter class=\"solr.StopFilterFactory\" ignoreCase=\"true\" words=\"stopwords.txt\" enablePositionIncrements=\"true\" />
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

-define(BAD_UK_SCHEMA,
        <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
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
   <field name=\"text\" type=\"text_general\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>
</fields>

 <uniqueKey>foobar</uniqueKey>

<types>
    <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"text_general\" class=\"solr.TextField\" positionIncrementGap=\"100\">
      <analyzer type=\"index\">
        <tokenizer class=\"solr.StandardTokenizerFactory\"/>
        <filter class=\"solr.StopFilterFactory\" ignoreCase=\"true\" words=\"stopwords.txt\" enablePositionIncrements=\"true\" />
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

%% Change _yz_id to type `string'
-define(BAD_YZ_FIELD_SCHEMA,
        <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"_yz_id\" type=\"string\" indexed=\"true\" stored=\"true\" required=\"true\" />
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_node\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"text\" type=\"text_general\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>
</fields>

 <uniqueKey>_yz_id</uniqueKey>

<types>
    <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"text_general\" class=\"solr.TextField\" positionIncrementGap=\"100\">
      <analyzer type=\"index\">
        <tokenizer class=\"solr.StandardTokenizerFactory\"/>
        <filter class=\"solr.StopFilterFactory\" ignoreCase=\"true\" words=\"stopwords.txt\" enablePositionIncrements=\"true\" />
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

-define(BAD_CLASS_SCHEMA,
        <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" />
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_node\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"text\" type=\"text_general\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>
</fields>

 <uniqueKey>_yz_id</uniqueKey>

<types>
    <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"text_general\" class=\"solr.TextFieldFoo\" positionIncrementGap=\"100\">
      <analyzer type=\"index\">
        <tokenizer class=\"solr.StandardTokenizerFactory\"/>
        <filter class=\"solr.StopFilterFactory\" ignoreCase=\"true\" words=\"stopwords.txt\" enablePositionIncrements=\"true\" />
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
    Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    confirm_create_schema(Cluster, <<"test_schema">>, ?TEST_SCHEMA),
    confirm_get_schema(Cluster, <<"test_schema">>, ?TEST_SCHEMA),
    confirm_not_found(Cluster, <<"not_a_schema">>),
    confirm_bad_ct(Cluster, <<"bad_ct">>, ?TEST_SCHEMA),
    confirm_truncated(Cluster, <<"truncated">>, ?TRUNCATED_SCHEMA),
    confirm_missing_yz_fields(Cluster, <<"missing_yz_fields">>, ?MISSING_YZ_FIELDS_SCHEMA),
    confirm_bad_uk(Cluster, <<"bad_uk">>, ?BAD_UK_SCHEMA),
    confirm_bad_yz_field(Cluster, <<"bad_yz_field">>, ?BAD_YZ_FIELD_SCHEMA),
    confirm_default_schema(Cluster, <<"default">>, default_schema(Cluster)),
    confirm_bad_schema(Cluster),
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
    {ok, Status, _, _Body} = http(put, URL, Headers, RawSchema),
    ?assertEqual("415", Status).

%% @doc Confirm that truncated schema fails, returning 400.
confirm_truncated(Cluster, Name, RawSchema) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_truncated ~s [~p]", [Name, HP]),
    URL = schema_url(HP, Name),
    Headers = [{"content-type", "application/xml"}],
    {ok, Status, _, Body} = http(put, URL, Headers, RawSchema),
    ?assertEqual("400", Status),
    %% assert the body contains some kind of msg as to why the schema
    %% failed to parse
    ?assert(size(Body) > 0).

%% @doc Confirm missing `_yz' fields return 400.
confirm_missing_yz_fields(Cluster, Name, RawSchema) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_missing_yz_fields ~s [~p]", [Name, HP]),
    URL = schema_url(HP, Name),
    Headers = [{"content-type", "application/xml"}],
    {ok, Status, _, Body} = http(put, URL, Headers, RawSchema),
    ?assertEqual("400", Status),
    ?assert(size(Body) > 0).

%% @doc Confirm bad `uniqueKey' returns 400.
confirm_bad_uk(Cluster, Name, RawSchema) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_bad_uk ~s [~p]", [Name, HP]),
    URL = schema_url(HP, Name),
    Headers = [{"content-type", "application/xml"}],
    {ok, Status, _, Body} = http(put, URL, Headers, RawSchema),
    ?assertEqual("400", Status),
    ?assert(size(Body) > 0).

%% @doc Confirm a bad yz field returns 400.
confirm_bad_yz_field(Cluster, Name, RawSchema) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_bad_yz_field ~s [~p]", [Name, HP]),
    URL = schema_url(HP, Name),
    Headers = [{"content-type", "application/xml"}],
    {ok, Status, _, Body} = http(put, URL, Headers, RawSchema),
    ?assertEqual("400", Status),
    ?assert(size(Body) > 0).

%% @doc Confirm that the default schema passes verification.  Yokozuna
%%      assumes the default schema is correct so this test is useful
%%      for catching regression between the default schema and
%%      verification code.
confirm_default_schema(Cluster, Name, RawSchema) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_default_schema ~s [~p]", [Name, HP]),
    URL = schema_url(HP, Name),
    Headers = [{"content-type", "application/xml"}],
    {ok, Status, _, _} = http(put, URL, Headers, RawSchema),
    ?assertEqual("204", Status).

%% @doc Confirm that an index created with a bad schema causing a Solr
%%      runtime error doesn't crash the cluster.  Furthermore, verify
%%      that the index is eventually created after the schema is fixed.
confirm_bad_schema(Cluster) ->
    Name = <<"bad_class">>,
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_bad_schema ~s [~p]", [Name, HP]),
    URL = schema_url(HP, Name),
    Headers = [{"content-type", "application/xml"}],
    {ok, Status, _, _} = http(put, URL, Headers, ?BAD_CLASS_SCHEMA),
    ?assertEqual("204", Status),

    URL2 = index_url(HP, Name),
    Headers2 = [{"content-type", "application/json"}],
    Body = {struct, [{schema, Name}]},
    lager:info("create ~s index using ~s schema", [Name, Name]),
    {ok, Status2, _, _} = http(put, URL2, Headers2, mochijson2:encode(Body)),
    %% This should return 204 because it simply adds an entry to the
    %% ring.  The actual Solr Core creation is async.
    ?assertEqual("204", Status2),

    lager:info("give solr time to attempt to create core ~s", [Name]),
    timer:sleep(5000),
    lager:info("verify solr core ~s is not up", [Name]),
    ?assertNot(yz_solr:ping(binary_to_list(Name))),

    lager:info("upload corrected schema ~s", [Name]),
    {ok, Status3, _, _} = http(put, URL, Headers, ?TEST_SCHEMA),
    ?assertEqual("204", Status3),

    lager:info("wait for yz to retry creation of core ~s", [Name]),
    Node = select_random(Cluster),
    F = fun(Node2) ->
                lager:info("try to ping core ~s", [Name]),
                rpc:call(Node2, yz_solr, ping, [binary_to_list(Name)])
        end,
    ?assertEqual(ok, rt:wait_until(Node, F)).


%%%===================================================================
%%% Helpers
%%%===================================================================

ct(Headers) ->
    Headers2 = [{string:to_lower(Key), Value} || {Key, Value} <- Headers],
    proplists:get_value("content-type", Headers2).

default_schema(Cluster) ->
    Node = select_random(Cluster),
    lager:info("get default schema from ~p", [Node]),
    {ok, RawSchema} = rpc:call(Node, yz_schema, get, [<<"_yz_default">>]),
    RawSchema.

http(Method, URL, Headers, Body) ->
    Opts = [{response_format, binary}],
    ibrowse:send_req(URL, Headers, Method, Body, Opts).

index_url({Host,Port}, Name) ->
    ?FMT("http://~s:~B/yz/index/~s", [Host, Port, Name]).

schema_url({Host,Port}, Name) ->
    ?FMT("http://~s:~B/yz/schema/~s", [Host, Port, Name]).
