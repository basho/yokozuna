%% @doc Test the index adminstration API in various ways.
-module(yz_index_admin).
-compile(export_all).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Copied from rt.erl, would be nice if there was a rt.hrl
-type interface() :: {http, tuple()} | {pb, tuple()}.
-type interfaces() :: [interface()].
-type conn_info() :: [{node(), interfaces()}].
-type cluster() :: [node()].

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(NO_HEADERS, []).
-define(NO_BODY, <<>>).
-define(CFG,
        [
         {riak_core,
          [
           {ring_creation_size, 8}
          ]},
         {yokozuna,
          [
           {enabled, true}
          ]}
        ]).

-define(SCHEMA,
        <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" />
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\"/>
   <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_err\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
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

-define(SCHEMA_FIELD_ADDED,
        <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" />
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\"/>
   <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"_yz_err\" type=\"_yz_str\" indexed=\"true\" stored=\"true\"/>
   <field name=\"text\" type=\"text_general\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>
   <field name=\"my_new_field\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" />
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

confirm() ->
    Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    confirm_create_index_1(Cluster),
    confirm_create_index_2(Cluster),
    confirm_409(Cluster),
    confirm_list(Cluster, [<<"test_index_1">>, <<"test_index_2">>, <<"test_index_409">>]),
    confirm_delete(Cluster, <<"test_index_1">>),
    confirm_get(Cluster, <<"test_index_2">>),
    confirm_404(Cluster, <<"not_an_index">>),
    confirm_delete_409(Cluster, <<"delete_409">>),
    confirm_field_add(Cluster, <<"field_add">>),
    pass.

%% @doc Test basic creation, no body.
confirm_create_index_1(Cluster) ->
    Index = <<"test_index_1">>,
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_create_index_1 ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    {ok, Status, _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("204", Status),
    yz_rt:wait_for_index(Cluster, Index).

%% @doc Test index creation when passing schema name.
confirm_create_index_2(Cluster) ->
    Index = <<"test_index_2">>,
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_create_index_2 ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    Headers = [{"content-type", "application/json"}],
    Body = <<"{\"schema\":\"_yz_default\"}">>,
    {ok, Status, _, _} = http(put, URL, Headers, Body),
    ?assertEqual("204", Status),
    yz_rt:wait_for_index(Cluster, Index).

confirm_409(Cluster) ->
    Index = <<"test_index_409">>,
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_409 ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    {ok, Status1, _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("204", Status1),
    yz_rt:wait_for_index(Cluster, Index),
    {ok, Status2, _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("409", Status2).

confirm_list(Cluster, Indexes) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_list ~p [~p]", [Indexes, HP]),
    URL = index_list_url(HP),
    {ok, "200", _, Body} = http(get, URL, ?NO_HEADERS, ?NO_BODY),
    check_list(Indexes, Body).

confirm_delete(Cluster, Index) ->
    confirm_get(Cluster, Index),
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_delete ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),

    Suffix = "search/index/" ++ Index,
    Is200 = is_status("200", get, Suffix, ?NO_HEADERS, ?NO_BODY),
    [ ?assertEqual(ok, rt:wait_until(Node, Is200)) || Node <- Cluster],

    %% Only run DELETE on one node
    {ok, Status2, _, _} = http(delete, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("204", Status2),

    Is404 = is_status("404", get, Suffix, ?NO_HEADERS, ?NO_BODY),
    [ ?assertEqual(ok, rt:wait_until(Node, Is404)) || Node <- Cluster],

    %% Verify the index dir was removed from disk as well
    [ ?assertEqual(ok, rt:wait_until(Node, is_deleted("data/search/" ++ binary_to_list(Index))))
      || Node <- Cluster].

is_status(ExpectedStatus, Method, URLSuffix, Headers, Body) ->
    fun(Node) ->
            HP = hd(host_entries(rt:connection_info([Node]))),
            URL = url(HP, URLSuffix),
            lager:info("checking for expected status ~p from ~p ~p",
                       [ExpectedStatus, Method, URL]),
            {ok, Status, _, _} = http(Method, URL, Headers, Body),
            ExpectedStatus == Status
    end.

is_deleted(Dir) ->
    fun(Node) ->
            lager:info("checking if dir ~p is deleted for node ~p", [Dir, Node]),
            not rpc:call(Node, filelib, is_dir, [Dir])
    end.

confirm_get(Cluster, Index) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_get ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    {ok, Status, Headers, _} = http(get, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("200", Status),
    ?assertEqual("application/json", ct(Headers)).

confirm_404(Cluster, Index) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("confirm_404 ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    {ok, Status, _, _} = http(get, URL, ?NO_HEADERS, ?NO_BODY),
    ?assertEqual("404", Status).

%% @doc Confirm that an index with 1 or more buckets associated with
%%      it is not allowed to be deleted.
confirm_delete_409(Cluster, Index) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    lager:info("Verify that index ~s cannot be deleted b/c of associated buckets [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    {ok, "204", _, _} = http(put, URL, ?NO_HEADERS, ?NO_BODY),
    H = [{"content-type", "application/json"}],
    B = <<"{\"props\":{\"search_index\":\"delete_409\"}}">>,
    {ok, "204", _, _} = http(put, bucket_url(HP, <<"b1">>), H, B),
    {ok, "204", _, _} = http(put, bucket_url(HP, <<"b2">>), H, B),

    yz_rt:wait_for_index(Cluster, Index),
    %% Sleeping for bprops (TODO: convert to `wait_for_bprops')
    timer:sleep(4000),

    %% Verify associated_buckets is correct
    Node = select_random(Cluster),
    lager:info("Verify associated buckets for index ~s [~p]", [Node]),
    Ring = rpc:call(Node, yz_misc, get_ring, [transformed]),
    Assoc = rpc:call(Node, yz_index, associated_buckets, [Index, Ring]),
    ?assertEqual([<<"b1">>, <<"b2">>], Assoc),

    %% Can't be deleted because of associated buckets
    {ok, "409", _, _} = http(delete, URL, ?NO_HEADERS, ?NO_BODY),

    B2 = <<"{\"props\":{\"search_index\":\"_yz_default\"}}">>,
    {ok, "204", _, _} = http(put, bucket_url(HP, <<"b2">>), H, B2),

    %% Still can't delete because of associated bucket
    {ok, "409", _, _} = http(delete, URL, ?NO_HEADERS, ?NO_BODY),

    B2 = <<"{\"props\":{\"search_index\":\"_yz_default\"}}">>,
    {ok, "204", _, _} = http(put, bucket_url(HP, <<"b1">>), H, B2),

    %% TODO: wait_for_index_delete?
    {ok, "204", _, _} = http(delete, URL, ?NO_HEADERS, ?NO_BODY).

%% @doc Verify that an index's schema can have a field added to it and
%% reloaded.
confirm_field_add(Cluster, Index) ->
    CI = yz_rt:connection_info(Cluster),
    RandCI = select_random(CI),
    HP = yz_rt:riak_http(RandCI),
    lager:info("confirm_field_add"),

    lager:info("upload schema ~s [~p]", [Index, HP]),
    SchemaURL = schema_url(HP, Index),
    SchemaHeaders = [{"content-type", "application/xml"}],
    {ok, Status1, _, _} = http(put, SchemaURL, SchemaHeaders, ?SCHEMA),
    ?assertEqual("204", Status1),

    lager:info("create index ~s using schema ~s [~p]", [Index, Index, HP]),
    IndexURL = index_url(HP, Index),
    IndexHeaders = [{"content-type", "application/json"}],
    Body = <<"{\"schema\":\"field_add\"}">>,
    {ok, Status2, _, _} = http(put, IndexURL, IndexHeaders, Body),
    ?assertEqual("204", Status2),

    yz_rt:wait_for_index(Cluster, Index),

    Node = select_random(Cluster),
    FieldURL = field_url(yz_rt:solr_http(RandCI), Index, "my_new_field"),
    lager:info("verify index ~s doesn't have my_new_field [~p]", [Index, Node]),
    {ok, "404", _, _} = http(get, FieldURL, ?NO_HEADERS, ?NO_BODY),

    lager:info("upload schema ~s with new field [~p]", [Index, HP]),
    {ok, Status3, _, _} = http(put, SchemaURL, SchemaHeaders, ?SCHEMA_FIELD_ADDED),
    ?assertEqual("204", Status3),

    lager:info("reload index ~s [~p]", [Index, Node]),
    {ok, _} = rpc:call(Node, yz_index, reload, [Index]),

    yz_rt:wait_until(Cluster, field_exists(Index, "my_new_field", CI)).

%%%===================================================================
%%% Helpers
%%%===================================================================

bucket_url({Host, Port}, Bucket) ->
    ?FMT("http://~s:~B/buckets/~s/props", [Host, Port, Bucket]).

check_list(Indexes, Body) ->
    Decoded = mochijson2:decode(Body),
    Names = [proplists:get_value(<<"name">>, Obj) || {struct, Obj} <- Decoded],
    ?assertEqual(lists:sort(Indexes), lists:sort(Names)).

contains_index(_Body) ->
    undefined.

ct(Headers) ->
    Headers2 = [{string:to_lower(Key), Value} || {Key, Value} <- Headers],
    proplists:get_value("content-type", Headers2).

-spec field_exists(index_name(), string(), conn_info()) -> predicate(node()).
field_exists(Index, Field, ConnInfo) ->
    fun(Node) ->
            HP = yz_rt:solr_http(proplists:get_value(Node, ConnInfo)),
            URL = field_url(HP, Index, Field),
            lager:info("verify ~s added ~s", [Index, URL]),
            {ok, Status, _, _} = http(get, URL, ?NO_HEADERS, ?NO_BODY),
            Status == "200"
    end.

field_url({Host,Port}, Index, FieldName) ->
    ?FMT("http://~s:~B/solr/~s/schema/fields/~s", [Host, Port, Index, FieldName]).

host_entries(ClusterConnInfo) ->
    [proplists:get_value(http, I) || {_,I} <- ClusterConnInfo].

http(Method, URL, Headers, Body) ->
    Opts = [],
    ibrowse:send_req(URL, Headers, Method, Body, Opts).

index_list_url({Host, Port}) ->
    ?FMT("http://~s:~B/search/index", [Host, Port]).

index_url({Host,Port}, Index) ->
    ?FMT("http://~s:~B/search/index/~s", [Host, Port, Index]).

schema_url({Host,Port}, Name) ->
    ?FMT("http://~s:~B/search/schema/~s", [Host, Port, Name]).

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

url({Host,Port}, Suffix) ->
    ?FMT("http://~s:~B/~s", [Host, Port, Suffix]).
