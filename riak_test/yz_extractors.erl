%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%-------------------------------------------------------------------

%% @doc Test that checks if we're caching the extractor map and that
%%      creating custom extractors is doable via protobufs.
%% @end

-module(yz_extractors).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(TYPE1, <<"extractors_in_paradise">>).
-define(TYPE2, <<"extractors_in_paradiso">>).
-define(INDEX1, <<"test_idx1">>).
-define(BUCKET1, {?TYPE1, <<"test_bkt1">>}).
-define(INDEX2, <<"test_idx2">>).
-define(BUCKET2, {?TYPE2, <<"test_bkt2">>}).
-define(TYPE3, <<"type3">>).
-define(BUCKET3, {?TYPE3, <<"test_bkt3">>}).
-define(INDEX3, <<"test_idx3">>).
-define(SCHEMANAME, <<"test">>).
-define(TEST_SCHEMA,
<<"<schema name=\"test\" version=\"1.5\">
<fields>
   <dynamicField name=\"*_foo_register\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <dynamicField name=\"*\" type=\"ignored\"/>

   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_err\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"text\" type=\"text_general\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>
   <field name=\"age\" type=\"int\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"host\" type=\"string\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"blob\" type=\"binary\" indexed=\"false\" stored=\"true\" multiValued=\"false\"/>
</fields>
<uniqueKey>_yz_id</uniqueKey>
<types>
    <fieldType name=\"ignored\" indexed=\"false\" stored=\"false\" multiValued=\"false\" class=\"solr.StrField\" />

    <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"string\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldtype name=\"binary\" class=\"solr.BinaryField\"/>
    <fieldType name=\"int\" class=\"solr.TrieIntField\" precisionStep=\"0\" positionIncrementGap=\"0\" />
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
-define(TEST_SCHEMA_UPGRADE,
<<"<schema name=\"test\" version=\"1.5\">
<fields>
   <dynamicField name=\"*_foo_register\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <dynamicField name=\"*\" type=\"ignored\"/>

   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_err\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"text\" type=\"text_general\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>
   <field name=\"age\" type=\"int\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"host\" type=\"string\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"blob\" type=\"binary\" indexed=\"false\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"method\" type=\"string\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
</fields>
<uniqueKey>_yz_id</uniqueKey>
<types>
    <fieldType name=\"ignored\" indexed=\"false\" stored=\"false\" multiValued=\"false\" class=\"solr.StrField\" />

    <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"string\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldtype name=\"binary\" class=\"solr.BinaryField\"/>
    <fieldType name=\"int\" class=\"solr.TrieIntField\" precisionStep=\"0\" positionIncrementGap=\"0\" />
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
-define(YZ_CAP, {yokozuna, extractor_map_in_cmd}).
-define(GET_MAP_RING_MFA, {yz_extractor, get_map, 1}).
-define(GET_MAP_MFA, {yz_extractor, get_map, 0}).
-define(GET_MAP_READTHROUGH_MFA, {yz_extractor, get_map_read_through, 0}).
-define(YZ_META_EXTRACTORS, {yokozuna, extractors}).
-define(YZ_EXTRACTOR_MAP, yokozuna_extractor_map).
-define(NEW_EXTRACTOR, {"application/httpheader", yz_noop_extractor}).
-define(EXTRACTOR_CT, element(1, ?NEW_EXTRACTOR)).
-define(EXTRACTOR_MOD, element(2, ?NEW_EXTRACTOR)).
-define(DEFAULT_MAP, [{default, yz_noop_extractor},
                      {"application/json",yz_json_extractor},
                      {"application/riak_counter", yz_dt_extractor},
                      {"application/riak_map", yz_dt_extractor},
                      {"application/riak_set", yz_dt_extractor},
                      {"application/xml",yz_xml_extractor},
                      {"text/plain",yz_text_extractor},
                      {"text/xml",yz_xml_extractor}
                     ]).
-define(EXTRACTMAPEXPECT, lists:sort(?DEFAULT_MAP ++ [?NEW_EXTRACTOR])).
-define(SEQMAX, 20).
-define(NVAL, 3).
-define(CFG,
        [
         {riak_kv,
          [
           %% allow AAE to build trees and exchange rapidly
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 8},
           {anti_entropy_tick, 1000},
           %% but start with AAE turned off so as not to interfere with earlier parts of the test
           {anti_entropy, {off, []}}
          ]},
         {yokozuna,
          [
           {enabled, true}
          ]}
        ]).

confirm() ->
    %% This test explicitly requires an upgrade from 2.0.5 to test a
    %% new capability
    OldVsn = "2.0.5",

    [_, Node|_] = Cluster = rt:build_cluster(lists:duplicate(4, {OldVsn, ?CFG})),
    rt:wait_for_cluster_service(Cluster, yokozuna),

    [rt:assert_capability(ANode, ?YZ_CAP, {unknown_capability, ?YZ_CAP}) || ANode <- Cluster],

    OldPid = rt:pbc(Node),

    GenKeys = yokozuna_rt:gen_keys(?SEQMAX),
    KeyCount = length(GenKeys),

    rt:count_calls(Cluster, [?GET_MAP_RING_MFA, ?GET_MAP_MFA]),

    yokozuna_rt:write_data(Cluster, OldPid, ?INDEX1,
                           {?SCHEMANAME, ?TEST_SCHEMA}, ?BUCKET1, GenKeys),

    ok = rt:stop_tracing(),

    {ok, BProps} = riakc_pb_socket:get_bucket(OldPid, ?BUCKET1),
    N = proplists:get_value(n_val, BProps),

    riakc_pb_socket:stop(OldPid),

    PrevGetMapRingCC = rt:get_call_count(Cluster, ?GET_MAP_RING_MFA),
    PrevGetMapCC = rt:get_call_count(Cluster, ?GET_MAP_MFA),
    ?assertEqual(KeyCount * N, PrevGetMapRingCC),
    ?assertEqual(KeyCount * N, PrevGetMapCC),

    %% test query count
    yokozuna_rt:verify_num_found_query(Cluster, ?INDEX1, KeyCount),

    {RingVal1, MDVal1} = get_ring_and_cmd_vals(Node, ?YZ_META_EXTRACTORS,
                                               ?YZ_EXTRACTOR_MAP),

    ?assertEqual(undefined, MDVal1),
    %% In previous version, Ring only gets map metadata if a non-default
    %% extractor is registered
    ?assertEqual(undefined, RingVal1),

    ?assertEqual(?DEFAULT_MAP, get_map(Node)),

    %% %% Custom Register
    ExtractMap = register_extractor(Node, ?EXTRACTOR_CT, ?EXTRACTOR_MOD),

    ?assertEqual(?EXTRACTMAPEXPECT, ExtractMap),

    %% Upgrade
    yokozuna_rt:rolling_upgrade(Cluster, current),

    [rt:wait_until_ready(ANode) || ANode <- Cluster],

    [rt:assert_capability(ANode, ?YZ_CAP, true) || ANode <- Cluster],
    [rt:assert_supported(rt:capability(ANode, all), ?YZ_CAP, [true, false]) ||
        ANode <- Cluster],

    %% test query count again
    yokozuna_rt:verify_num_found_query(Cluster, ?INDEX1, KeyCount),

    Pid = rt:pbc(Node),

    rt:count_calls(Cluster, [?GET_MAP_RING_MFA, ?GET_MAP_MFA,
                             ?GET_MAP_READTHROUGH_MFA]),

    yokozuna_rt:write_data(Cluster, Pid, ?INDEX2, {?SCHEMANAME, ?TEST_SCHEMA},
                           ?BUCKET2, GenKeys),
    yokozuna_rt:commit(Cluster, ?INDEX2),

    ok = rt:stop_tracing(),

    riakc_pb_socket:stop(Pid),

    CurrGetMapRingCC = rt:get_call_count(Cluster, ?GET_MAP_RING_MFA),
    CurrGetMapCC = rt:get_call_count(Cluster, ?GET_MAP_MFA),
    CurrGetMapRTCC = rt:get_call_count(Cluster, ?GET_MAP_READTHROUGH_MFA),

    lager:info("Number of calls to get the map from the ring - current: ~p~n, previous: ~p~n",
              [CurrGetMapRingCC, PrevGetMapRingCC]),
    ?assert(CurrGetMapRingCC < PrevGetMapRingCC),
    lager:info("Number of calls to get the map - current: ~p~n, previous: ~p~n",
               [CurrGetMapCC, PrevGetMapCC]),
    ?assert(CurrGetMapCC =< PrevGetMapCC),
    lager:info("Number of calls to get_map_read_through/0: ~p~n, Number of calls to get_map/0: ~p~n",
              [CurrGetMapRTCC, CurrGetMapCC]),
    ?assert(CurrGetMapRTCC =< CurrGetMapCC),

    {_RingVal2, MDVal2} = get_ring_and_cmd_vals(Node, ?YZ_META_EXTRACTORS,
                                                ?YZ_EXTRACTOR_MAP),

    ?assertEqual(?EXTRACTMAPEXPECT, MDVal2),
    ?assertEqual(?EXTRACTMAPEXPECT, get_map(Node)),

    Packet =  <<"GET http://www.google.com HTTP/1.1\n">>,
    test_extractor_works(Cluster, Packet),
    test_extractor_with_aae_expire(Cluster, ?INDEX2, ?BUCKET2, Packet),
    test_bad_extraction(Cluster),

    pass.

%%%===================================================================
%%% Private
%%%===================================================================

get_ring_and_cmd_vals(Node, Prefix, Key) ->
    Ring = rpc:call(Node, yz_misc, get_ring, [transformed]),
    MDVal = metadata_get(Node, Prefix, Key),
    RingVal = ring_meta_get(Node, Key, Ring),
    {RingVal, MDVal}.

metadata_get(Node, Prefix, Key) ->
    rpc:call(Node, riak_core_metadata, get, [Prefix, Key, []]).

ring_meta_get(Node, Key, Ring) ->
    rpc:call(Node, riak_core_ring, get_meta, [Key, Ring]).

register_extractor(Node, MimeType, Mod) ->
    rpc:call(Node, yz_extractor, register, [MimeType, Mod]).

get_map(Node) ->
    rpc:call(Node, yz_extractor, get_map, []).

verify_extractor(Node, PacketData, Mod) ->
    rpc:call(Node, yz_extractor, run, [PacketData, Mod]).

bucket_url({Host,Port}, {BType, BName}, Key) ->
    ?FMT("http://~s:~B/types/~s/buckets/~s/keys/~s",
         [Host, Port, BType, BName, Key]).

test_extractor_works(Cluster, Packet) ->
    [rt_intercept:add(ANode, {yz_noop_extractor,
                              [{{extract, 1}, extract_httpheader}]}) ||
        ANode <- Cluster],
    [rt_intercept:wait_until_loaded(ANode) || ANode <- Cluster],

    ExpectedExtraction = [{method, 'GET'},
                          {host, <<"www.google.com">>},
                          {uri, <<"/">>}],
    ?assertEqual(ExpectedExtraction,
                 verify_extractor(rt:select_random(Cluster), Packet, ?EXTRACTOR_MOD)).

test_extractor_with_aae_expire(Cluster, Index, Bucket, Packet) ->
    %% Now make sure we register extractor across all nodes
    [register_extractor(ANode, ?EXTRACTOR_CT, ?EXTRACTOR_MOD) ||
        ANode <- Cluster],

    Key = <<"google">>,

    {Host, Port} = rt:select_random(yokozuna_rt:host_entries(
                                               rt:connection_info(
                                                 Cluster))),
    URL = bucket_url({Host, Port}, Bucket,
                     mochiweb_util:quote_plus(Key)),

    CT = ?EXTRACTOR_CT,
    {ok, "204", _, _} = yokozuna_rt:http(
        put, URL, [{"Content-Type", CT}], Packet),

    yokozuna_rt:commit(Cluster, Index),

    ANode = rt:select_random(Cluster),
    yokozuna_rt:search_expect(ANode, Index, <<"host">>,
                              <<"www*">>, 1),

    rpc:multicall(Cluster, riak_kv_entropy_manager, enable, []),

    yokozuna_rt:expire_trees(Cluster),
    yokozuna_rt:wait_for_full_exchange_round(Cluster, erlang:now()),

    yokozuna_rt:search_expect(ANode, Index, <<"host">>,
                              <<"www*">>, 1),

    APid = rt:pbc(rt:select_random(Cluster)),
    yokozuna_rt:override_schema(APid, Cluster, Index, ?SCHEMANAME,
                                ?TEST_SCHEMA_UPGRADE),

    {ok, "200", RHeaders, _} = yokozuna_rt:http(get, URL, [{"Content-Type", CT}],
                                                [], []),
    VC = proplists:get_value("X-Riak-Vclock", RHeaders),

    {ok, "204", _, _} = yokozuna_rt:http(
                          put, URL, [{"Content-Type", CT}, {"X-Riak-Vclock", VC}],
                          Packet),
    yokozuna_rt:commit(Cluster, Index),

    yokozuna_rt:search_expect(ANode, Index, <<"method">>,
                              <<"GET">>, 1),

    yokozuna_rt:expire_trees(Cluster),
    yokozuna_rt:wait_for_full_exchange_round(Cluster, erlang:now()),

    yokozuna_rt:search_expect(ANode, Index, <<"method">>,
                              <<"GET">>, 1),
    riakc_pb_socket:stop(APid).

test_bad_extraction(Cluster) ->
    %% Previous test enabled AAE, which makes the number of repairs here not consistent
    %% Turn off AAE again just to make the test deterministic.
    rpc:multicall(Cluster, riak_kv_entropy_manager, disable, []),
    %%
    %% register the no-op extractor on all the nodes with a content type
    %%
    [register_extractor(ANode, "application/bad-extractor", yz_noop_extractor) ||
        ANode <- Cluster],
    %%
    %% Set up the intercepts so that they extract non-unicode data
    %%
    [rt_intercept:add(ANode, {yz_noop_extractor,
        [{{extract, 1}, extract_non_unicode_data}]}) ||
        ANode <- Cluster],
    [rt_intercept:wait_until_loaded(ANode) || ANode <- Cluster],
    %%
    %% create and wire up the bucket to the Solr index/core
    %%
    yokozuna_rt:create_indexed_bucket_type(Cluster, ?TYPE3, ?INDEX3, ?SCHEMANAME),
    %%
    %% Grab the stats before
    %%
    {PreviousFailCount, PreviousErrorThresholdCount} = get_error_stats(Cluster),
    %%
    %% Put some  data into Riak.  This should cause the intercepted no-op
    %% extractor to generate an object to be written into Solr that contains
    %% non-unicode data.
    {Host, Port} = rt:select_random(
        yokozuna_rt:host_entries(rt:connection_info(Cluster))),
    Key = <<"test_bad_extraction">>,
    URL = bucket_url({Host, Port}, ?BUCKET3, Key),
    Headers = [{"Content-Type", "application/bad-extractor"}],
    Data =  <<"blahblahblahblah">>,
    {ok, "204", _, _} = yokozuna_rt:http(put, URL, Headers, Data),
    %%
    %% The put should pass, but because it's "bad data", there should
    %% be no data in Riak.
    %%
    yokozuna_rt:verify_num_found_query(Cluster, ?INDEX3, 0),
    %%
    %% Verify the stats.  There should be one more index failure,
    %% but there should be more more "melts" (error threshold failures)
    %%
    yokozuna_rt:wait_until(
        Cluster,
        fun(_Node) ->
            check_error_stats(Cluster, PreviousFailCount, PreviousErrorThresholdCount)
        end
    ),
    ok.

check_error_stats(Cluster, PreviousFailCount, PreviousErrorThresholdCount) ->
    {FailCount, ErrorThresholdCount} = get_error_stats(Cluster),
    lager:info(
        "PreviousFailCount: ~p FailCount: ~p;"
        " PreviousErrorThresholdCount: ~p; ErrorThresholdCount: ~p",
        [PreviousFailCount, FailCount,
         PreviousErrorThresholdCount, ErrorThresholdCount]
    ),
    PreviousFailCount + ?NVAL == FailCount
        andalso PreviousErrorThresholdCount == ErrorThresholdCount.


get_error_stats(Cluster) ->
    AllStats = [rpc:call(Node, yz_stat, get_stats, []) || Node <- Cluster],
    {
        lists:sum([get_count([index, bad_entry], count, Stats) || Stats <- AllStats]),
        lists:sum([get_count([search_index_error_threshold_failure_count], value, Stats) || Stats <- AllStats])
    }.

get_count(StatName, Type, Stats) ->
    proplists:get_value(
        Type,
        proplists:get_value(
            yz_stat:stat_name(StatName),
            Stats
        )
    ).
