%% @doc Test the index adminstration API in various ways.
-module(yz_languages).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

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

confirm() ->
    random:seed(now()),
    Cluster = rt:build_cluster(1, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    confirm_body_search_encoding(Cluster),
    confirm_language_field_type(Cluster),
    confirm_tag_encoding(Cluster),
    pass.

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

host_entries(ClusterConnInfo) ->
    [proplists:get_value(http, I) || {_,I} <- ClusterConnInfo].

index_url({Host,Port}, Index) ->
    ?FMT("http://~s:~B/yz/index/~s", [Host, Port, Index]).

bucket_url({Host,Port}, {BType, BName}, Key) ->
    ?FMT("http://~s:~B/types/~s/buckets/~s/keys/~s",
         [Host, Port, BType, BName, Key]).

http(Method, URL, Headers, Body) ->
    Opts = [],
    ibrowse:send_req(URL, Headers, Method, Body, Opts).

create_index(Cluster, HP, Index) ->
    lager:info("create_index ~s [~p]", [Index, HP]),
    URL = index_url(HP, Index),
    Headers = [{"content-type", "application/json"}],
    {ok, Status, _, _} = http(put, URL, Headers, ?NO_BODY),
    ok = yz_rt:set_bucket_type_index(hd(Cluster), Index),
    yz_rt:wait_for_index(Cluster, Index),
    ?assertEqual("204", Status).

store_and_search(Cluster, Bucket, Index, CT, Body, Field, Term) ->
    Headers = [{"Content-Type", CT}],
    store_and_search(Cluster, Bucket, Index, Headers, CT, Body, Field, Term).

store_and_search(Cluster, Bucket, Index, Headers, CT, Body, Field, Term) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    create_index(Cluster, HP, Index),
    URL = bucket_url(HP, Bucket, "test"),
    lager:info("Storing to bucket ~s", [URL]),
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body),
    %% Sleep for soft commit
    timer:sleep(1000),
    {ok, "200", _, Body} = ibrowse:send_req(URL, [{"accept", CT}], get, []),
    lager:info("Verify values are indexed"),
    ?assert(yz_rt:search_expect(HP, Index, Field, Term, 1)),
    ok.

confirm_body_search_encoding(Cluster) ->
    Index = <<"test_iso_8859_8">>,
    Bucket = {Index, <<"b">>},
    lager:info("confirm_iso_8859_8 ~s", [Index]),
    Body = "א בְּרֵאשִׁית, בָּרָא אֱלֹהִים, אֵת הַשָּׁמַיִם, וְאֵת הָאָרֶץ",
    store_and_search(Cluster, Bucket, Index, "text/plain", Body, "text", "בָּרָא").

confirm_language_field_type(Cluster) ->
    Index = <<"test_shift_jis">>,
    Bucket = {Index, <<"b">>},
    lager:info("confirm_shift_jis ~s", [Index]),
    Body = "{\"text_ja\" : \"私はハイビスカスを食べるのが 大好き\"}",
    store_and_search(Cluster, Bucket, Index, "application/json", Body, "text_ja", "大好き").

confirm_tag_encoding(Cluster) ->
    Index = <<"test_iso_8859_6">>,
    Bucket = {Index, <<"b">>},
    lager:info("confirm_iso_8859_6 ~s", [Index]),
    Body = "أردت أن أقرأ كتابا عن تاريخ المرأة في فرنسا",
    Headers = [{"Content-Type", "text/plain"},
               {"x-riak-meta-yz-tags", "x-riak-meta-arabic_s"},
               {"x-riak-meta-arabic_s", "أقرأ"}],
    store_and_search(Cluster, Bucket, Index, Headers, "text/plain", Body, "arabic_s", "أقرأ").
