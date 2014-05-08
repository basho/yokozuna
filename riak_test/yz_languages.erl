%% -*- coding: utf-8 -*-
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
           {enabled, true},
           {solr_startup_wait, 60}
          ]}
        ]).

confirm() ->
    random:seed(now()),
    Cluster = rt:build_cluster(1, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    confirm_body_search_encoding(Cluster),
    confirm_language_field_type(Cluster),
    confirm_tag_encoding(Cluster),
    confirm_type_bucket_key_encoding(Cluster),
    pass.

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

host_entries(ClusterConnInfo) ->
    [proplists:get_value(http, I) || {_,I} <- ClusterConnInfo].

index_url({Host,Port}, Index) ->
    ?FMT("http://~s:~B/search/index/~s", [Host, Port, Index]).

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
    yz_rt:wait_for_index(Cluster, Index),
    ok = yz_rt:set_bucket_type_index(hd(Cluster), Index),
    ?assertEqual("204", Status).

store_and_search(Cluster, Bucket, Index, CT, Body, Field, Term) ->
    Headers = [{"Content-Type", CT}],
    store_and_search(Cluster, Bucket, Index, Headers, CT, Body, Field, Term).

store_and_search(Cluster, Bucket, Index, Headers, CT, Body, Field, Term) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    create_index(Cluster, HP, Index),
    URL = bucket_url(HP, Bucket, "test"),
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body),
    %% Sleep for soft commit
    timer:sleep(1000),
    {ok, "200", _, ReturnedBody} = ibrowse:send_req(URL, [{"accept", CT}], get, []),
    ?assertEqual(Body, list_to_binary(ReturnedBody)),
    lager:info("Verify values are indexed"),
    ?assert(yz_rt:search_expect(HP, Index, Field, Term, 1)),
    ok.

store_and_search(Cluster, Language) ->
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    Index = type(Language),
    Body = body(Language),
    create_index(Cluster, HP, Index),
    URL = bucket_url(HP,
                     {mochiweb_util:quote_plus(Index),
                      mochiweb_util:quote_plus(bucket(Language))},
                     mochiweb_util:quote_plus(key(Language))),
    {ok, "204", _, _} = ibrowse:send_req(URL, headers(Language, put), put, Body),
    %% Sleep for soft commit
    timer:sleep(1000),
    {ok, "200", _, ReturnedBody} = ibrowse:send_req(URL, headers(Language, get), get, []),
    ?assertEqual(Body, list_to_binary(ReturnedBody)),
    lager:info("Verify values are indexed"),
    ?assert(yz_rt:search_expect(HP, Index, field(Language), term(Language), 1)),
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

confirm_type_bucket_key_encoding(Cluster) ->
    [begin
         lager:info("Testing ~p encoding", [L]),
         store_and_search(Cluster, L)
     end || L <- [arabic, hebrew]].
     %% Replace the preceding line with the following line once it is
     %% understood how to make non-utf-8 index names work
     %% correctly. Currently trying to use the Japanese index names
     %% results in an error as solr tries to parse it as utf-8.  end
     %% || L <- [arabic, japanese, hebrew]].

field(arabic) ->
    "arabic_s";
field(japanese) ->
    "text_ja";
field(hebrew) ->
    "text".

headers(arabic, put) ->
    [content_type(arabic),
     {"x-riak-meta-yz-tags", "x-riak-meta-arabic_s"},
     {"x-riak-meta-arabic_s", <<"أقرأ"/utf8>>}];
headers(L, put) ->
    [content_type(L)];
headers(japanese, get) ->
    [accept(japanese)];
headers(L, get) ->
    [accept(L), accept_charset(L)].

content_type(arabic) ->
    {"content-type", "text/plain; charset=ISO-8859-6"};
content_type(japanese) ->
    {"content-type", "application/json; charset=shift_jis"};
content_type(hebrew) ->
    {"content-type", "text/plain; charset=ISO-8859-8"}.

accept(japanese) ->
    {"accept", "application/json"};
accept(_) ->
    {"accept", "text/plain"}.

accept_charset(arabic) ->
    {"accept-charset", "ISO-8859-6"};
accept_charset(japanese) ->
    {"accept-charset", "shift_jis"};
accept_charset(hebrew) ->
    {"accept-charset", "ISO-8859-8"}.

type(arabic) ->
    <<"نوع">>;
type(japanese) ->
    <<"タイプ"/utf16>>;
type(hebrew) ->
    <<"סוג"/utf8>>.

bucket(arabic) ->
    <<"دلو">>;
bucket(japanese) ->
    <<"バケット"/utf16>>;
bucket(hebrew) ->
    <<"דלי"/utf8>>.

key(arabic) ->
    <<"مفتاح">>;
key(japanese) ->
    <<"キー"/utf16>>;
key(hebrew) ->
    <<"קָלִיד"/utf8>>.

body(arabic) ->
    <<"أردت أن أقرأ كتابا عن تاريخ المرأة في فرنسا"/utf8>>;
body(japanese) ->
    <<"{\"text_ja\" : \"私はハイビスカスを食べるのが 大好き\"}"/utf8>>;
body(hebrew) ->
    <<"א בְּרֵאשִׁית, בָּרָא אֱלֹהִים, אֵת הַשָּׁמַיִם, וְאֵת הָאָרֶץ"/utf8>>.

term(arabic) ->
    "أقرأ";
term(japanese) ->
    "大好き";
term(hebrew) ->
    "בָּרָא".
