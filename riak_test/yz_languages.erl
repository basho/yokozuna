%% -*- coding: utf-8 -*-
%% @doc Test the index adminstration API in various ways.
-module(yz_languages).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
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
    confirm_reserved_word_safety(Cluster),
    pass.

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

host_entries(ClusterConnInfo) ->
    [proplists:get_value(http, I) || {_,I} <- ClusterConnInfo].

bucket_url({Host,Port}, {BType, BName}, Key) ->
    ?FMT("http://~s:~B/types/~s/buckets/~s/keys/~s",
         [Host, Port, BType, BName, Key]).

store_and_search(Cluster, Bucket, Index, CT, Body, Field, Term) ->
    Headers = [{"Content-Type", CT}],
    store_and_search(Cluster, Bucket, Index, Headers, CT, Body, Field, Term).

store_and_search(Cluster, Bucket, Index, Headers, CT, Body, Field, Term) ->
    store_and_search(Cluster, Bucket, Index, "test", Headers, CT, Body, Field, Term).

store_and_search(Cluster, Bucket, Index, Key, Headers, CT, Body, Field, Term) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    yz_rt:create_index_http(Cluster, HP, Index),
    URL = bucket_url(HP, Bucket, Key),
    lager:info("Storing to bucket ~s", [URL]),
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body),

    %% wait for commit
    commit(Cluster, Index),

    {ok, "200", _, ReturnedBody} = ibrowse:send_req(URL, [{"accept", CT}], get, []),
    ?assertEqual(Body, list_to_binary(ReturnedBody)),
    lager:info("Verify values are indexed"),
    ?assert(yz_rt:search_expect(HP, Index, Field, Term, 1)),
    ok.

confirm_body_search_encoding(Cluster) ->
    Index = <<"test_iso_8859_8">>,
    Bucket = {Index, <<"b">>},
    lager:info("confirm_iso_8859_8 ~s", [Index]),
    Body = <<"א בְּרֵאשִׁית, בָּרָא אֱלֹהִים, אֵת הַשָּׁמַיִם, וְאֵת הָאָרֶץ"/utf8>>,
    store_and_search(Cluster, Bucket, Index, "text/plain", Body, "text", "בָּרָא").

confirm_language_field_type(Cluster) ->
    Index = <<"test_shift_jis">>,
    Bucket = {Index, <<"b">>},
    lager:info("confirm_shift_jis ~s", [Index]),
    Body = <<"{\"text_ja\" : \"私はハイビスカスを食べるのが 大好き\"}"/utf8>>,
    store_and_search(Cluster, Bucket, Index, "application/json", Body, "text_ja", "大好き").

confirm_tag_encoding(Cluster) ->
    Index = <<"test_iso_8859_6">>,
    Bucket = {Index, <<"b">>},
    lager:info("confirm_iso_8859_6 ~s", [Index]),
    Body = <<"أردت أن أقرأ كتابا عن تاريخ المرأة في فرنسا"/utf8>>,
    Headers = [{"Content-Type", "text/plain"},
               {"x-riak-meta-yz-tags", "x-riak-meta-arabic_s"},
               {"x-riak-meta-arabic_s", <<"أقرأ"/utf8>>}],
    store_and_search(Cluster, Bucket, Index, Headers, "text/plain", Body, "arabic_s", "أقرأ").

confirm_reserved_word_safety(Cluster) ->
    Index = <<"reserved">>,
    Bucket = {Index, <<"b">>},
    lager:info("confirm_reserved_word_safety ~s", [Index]),
    Body = <<"whatever">>,
    Headers = [{"Content-Type", "text/plain"}],
    RKey = "OR",
    store_and_search(Cluster, Bucket, Index, RKey, Headers, "text/plain", Body, "text", "whatever").

commit(Nodes, Index) ->
    %% Wait for yokozuna index to trigger, then force a commit
    timer:sleep(1000),
    rpc:multicall(Nodes, yz_solr, commit, [Index]).
