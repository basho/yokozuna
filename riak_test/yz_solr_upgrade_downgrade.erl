%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

-module(yz_solr_upgrade_downgrade).

-export([confirm/0]).

-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

%% internal exports needed for callbacks
-export([create_indexed_bucket_type/1]).

-define(CLUSTER_SIZE, 2).
-define(RING_SIZE, 8).
-define(N_VAL, 2).

-define(CONFIG, [
    {riak_core, [
        {ring_creation_size, 8},
        {handoff_concurrency, 10},
        {vnode_management_timer, 1000},
        {default_bucket_props, [{n_val, ?N_VAL}]}
    ]},
    {yokozuna, [
        {enabled, true},
        {anti_entropy_tick, 500},
        {anti_entropy_build_limit, {100, 1000}},
        {anti_entropy_concurrency, 8}
    ]}
]).

%% these indices is never upgraded
%% - index-a1 is not upgraded and is not written to after upgrade
%% - index-a1 is not upgraded and is written to after upgrade
-define(INDEX_A1, <<"index-a1">>).
-define(BUCKET_TYPE_A1, <<"bucket-type-a1">>).
-define(BUCKET_A1, {?BUCKET_TYPE_A1, <<"bucket-name-a1">>}).
-define(INDEX_A2, <<"index-a2">>).
-define(BUCKET_TYPE_A2, <<"bucket-type-a2">>).
-define(BUCKET_A2, {?BUCKET_TYPE_A2, <<"bucket-name-a2">>}).

%% each of these indices are upgraded:
%% - b1 is upgraded without any changes
%% - b2 is upgraded but the data is deleted
%% - b3 is upgraded but the data is backed up (for downgrade testing)
-define(INDEX_B1, <<"index-b1">>).
-define(BUCKET_TYPE_B1, <<"bucket-type-b1">>).
-define(BUCKET_B1, {?BUCKET_TYPE_B1, <<"bucket-name-b1">>}).
-define(INDEX_B2, <<"index-b2">>).
-define(BUCKET_TYPE_B2, <<"bucket-type-b2">>).
-define(BUCKET_B2, {?BUCKET_TYPE_B2, <<"bucket-name-b2">>}).
-define(INDEX_B3, <<"index-b3">>).
-define(BUCKET_TYPE_B3, <<"bucket-type-b3">>).
-define(BUCKET_B3, {?BUCKET_TYPE_B3, <<"bucket-name-b3">>}).

%% index-c is created after the upgrade
-define(INDEX_C, <<"index-c">>).
-define(BUCKET_TYPE_C, <<"bucket-type-c">>).
-define(BUCKET_C, {?BUCKET_TYPE_C, <<"bucket-name-c">>}).

-define(LUCENE_MATCH_VERSION_OPEN, "<luceneMatchVersion>").
-define(LUCENE_MATCH_4_7_VERSION, "4.7").
-define(LUCENE_MATCH_4_10_4_VERSION, "4.10.4").
-define(LUCENE_MATCH_VERSION_CLOSE, "</luceneMatchVersion>").


%%
%% This test exercises upgrade and downgrade of the underlying Solr server.
%% At the time of writing, this test assumes a previous version of Riak that
%% uses Solr 4.7, and a current version of Riak that uses Solr 4.10.4, the
%% latest (at the time of writing) Solr 4.x version.  These assumptions will
%% need to be revisited once we move to a 5.x or 6.x version of Solr.
%%
%% Users have the option of upgrading their indices to Solr 4.10.4.  Generally,
%% this involves:
%%
%% * modifying the `luceneMatchVersion` element in the solrconfig.xml file for
%%   each index to upgrade so that it matches the Solr version being upgraded to
%% * Deleting (or backing up) the Solr data associated with that index.
%%
%% Users are not required to upgrade their indices, if they do not want to.
%% If they chose to upgrade, they may upgrade a strict subset of their indices
%% and are not required to upgrade all of them.
%%
%% The test works by creating the following Solr indices
%%
%% * index-a1           an index the user does not upgrade, and which is not
%%                      written to after upgrade
%% * index-a2           an index the user does not upgrade, and which is
%%                      written to after upgrade
%% * index-b1           an index the user upgrades, but does not delete data
%% * index-b2           an index the user upgrades, and deletes data
%% * index-b3           an index the user upgrades, and backs up data
%%                      (for downgrade testing)
%%
%% After these indices are created and populated, one node in the cluster is
%% upgraded to current, and we test that after AAE has completed, all the missing
%% data is restored and is queryable.
%%
%% We then create:
%%
%% -index-c             an index created under Solr 4.10.4
%%
%% and verify to works as expected.
%%
%% We then downgrade back to previous (Solr 4.7), and verify that we need to
%% downgrade the version in solrconfig.xml and reindex any previously indexed
%% data in order for the downgraded cluster to behave properly (except for
%% index-a, which is not touched as part of the upgrade).
%%
confirm() ->
    %%
    %% Build a "previous" cluster using the configuration defined above
    %%
    Cluster = rt:build_cluster(lists:duplicate(
        ?CLUSTER_SIZE,
        {previous, ?CONFIG}
    )),
    %%
    %% Create all the indices, each of which is associated its own bucket type
    %%
    BucketProperties = [{n_val, ?N_VAL}],
    rt:pmap(
        fun create_indexed_bucket_type/1,
        [{Cluster, BucketType, Index, BucketProperties} ||
            {BucketType, Index} <-
                [{?BUCKET_TYPE_A1, ?INDEX_A1},
                 {?BUCKET_TYPE_A2, ?INDEX_A2},
                 {?BUCKET_TYPE_B1, ?INDEX_B1},
                 {?BUCKET_TYPE_B2, ?INDEX_B2},
                 {?BUCKET_TYPE_B3, ?INDEX_B3}]
        ]
    ),
    %%
    %% Write and verify the first 100 entries to our buckets
    %%
    rt:pmap(
        fun({Bucket, Index}) ->
            write_and_verify_data(Cluster, Bucket, Index, 1, 100, 100)
        end,
        [{?BUCKET_A1, ?INDEX_A1},
         {?BUCKET_A2, ?INDEX_A2},
         {?BUCKET_B1, ?INDEX_B1},
         {?BUCKET_B2, ?INDEX_B2},
         {?BUCKET_B3, ?INDEX_B3}]
    ),
    %%
    %% Upgrade dev1 to current, and in the process, upgrade the yz directory
    %% and index-b* indices that the luceneMatchVersion tag matches the version
    %% expected for Solr 4.10.4
    %%
    [Node1|[Node2 | _Rest]] = Cluster,
    NewConfig  = augment_config(yokozuna, solr_jmx_port, 44404, ?CONFIG),
    NewConfig2 = augment_config(yokozuna, enable_dist_query, false, NewConfig),
    NewConfig3 = augment_config(yokozuna, anti_entropy, {off, []}, NewConfig2),
    yz_rt:rolling_upgrade(
        Node1, current, NewConfig3, [riak_kv], fun update_yz/1
    ),
    %%
    %% Wait for Solr and all indices to be available in the cluster
    %%
    yz_rt:wait_for_solr(Cluster),
    [yz_rt:wait_for_index(Cluster, Index) ||
        Index <- [?INDEX_A1, ?INDEX_A2, ?INDEX_B1, ?INDEX_B2, ?INDEX_B3]
    ],
    %%
    %% Verify that the indices created so far (a*, b*) can be queried through
    %% the cluster, minus the node that is being upgraded, and that the data
    %% we deleted during upgrade is not there.
    %%
    [verify_data([Node2], Index, 100) ||
        Index <- [?INDEX_A1, ?INDEX_A2, ?INDEX_B1, ?INDEX_B2, ?INDEX_B3]
    ],
    [yz_rt:verify_num_match(solr, [Node1], Index, 100) ||
        Index <- [?INDEX_A1, ?INDEX_A2, ?INDEX_B1]
    ],
    [yz_rt:verify_num_match(solr, [Node1], Index, 0) ||
        Index <- [?INDEX_B2, ?INDEX_B3]
    ],
    %%
    %% add Node1 back to the set of candidates for cover plans,
    %% and turn YZ AAE back on.
    %%
    lager:info("Re-enabling dist_query on Node1 and turning on AAE ... "),
    rpc:call(Node1, yz_solr_proc, set_dist_query, [true]),
    rpc:call(Node1, application, set_env, [?YZ_APP_NAME, anti_entropy, {on, []}]),
    %%
    %% Wait for a full round of AAE
    %%
    yz_rt:wait_for_full_exchange_round(Cluster, erlang:now()),
    %%
    %% Verify that all missing Solr data has been repaired
    %%
    [verify_data(Cluster, Index, 100) ||
        Index <- [?INDEX_A1, ?INDEX_A2, ?INDEX_B1, ?INDEX_B2, ?INDEX_B3]
        ],
    %%
    %% Write and verify the next 100 entries to our buckets, except for
    %% index-a1, which we won't touch.
    %%
    rt:pmap(
        fun({Bucket, Index}) ->
            ok = write_and_verify_data(Cluster, Bucket, Index, 101, 100, 200)
        end,
        [{?BUCKET_A2, ?INDEX_A2},
         {?BUCKET_B1, ?INDEX_B1},
         {?BUCKET_B2, ?INDEX_B2},
         {?BUCKET_B3, ?INDEX_B3}]
    ),
    %%
    %% Create a new index (index-c) and bucket on the upgraded cluster, and
    %% verify it can hold data for us
    %%
    ok = yz_rt:create_indexed_bucket_type(
        Cluster, ?BUCKET_TYPE_C, ?INDEX_C, BucketProperties),
    ok = write_and_verify_data(Cluster, ?BUCKET_C, ?INDEX_C, 1, 200, 200),
    %%
    %% Downgrade dev1 back to previous, but don't downgrade the luceneMatch
    %% version for B* and C just yet.  Record the Params for finish_downgrade
    %% below
    %%
    NewConfig4  = augment_config(yokozuna, solr_jmx_port, 44405, ?CONFIG),
    DowngradeData = ets:new(downgrade_data, []),
    yz_rt:rolling_upgrade(
        Node1, previous, NewConfig4, [riak_kv],
        fun(Params) ->
            ets:insert(DowngradeData, {params, Params}),
            downgrade_yz(Params)
        end
    ),
    %%
    %% None of the indices we have tweaked should be available,
    %% except for index-a1, which we haven't touched.
    %%
    yz_rt:wait_for_solr(Cluster),
    yz_rt:wait_for_index(Cluster, ?INDEX_A1),
    ?assertEqual(
        lists:duplicate(5, false),
        [rpc:call(Node1, yz_index, exists, [Index]) ||
            Index <- [?INDEX_A2, ?INDEX_B1, ?INDEX_B2, ?INDEX_B3, ?INDEX_C]]
    ),
    %%
    %% Finish the downgrade by reverting the luceneMatchVersion and
    %% deleting the index data for all indices (including index-a!)
    %%
    rt:stop(Node1),
    [{params, Params}] = ets:lookup(DowngradeData, params),
    finish_downgrade(Params),
    rt:start(Node1),
    %%
    %% Wait for Solr and indices to come up
    %%
    yz_rt:wait_for_solr(Cluster),
    [yz_rt:wait_for_index(Cluster, Index) ||
        Index <- [?INDEX_A1, ?INDEX_A2, ?INDEX_B1, ?INDEX_B2, ?INDEX_B3, ?INDEX_C]
    ],
    %%
    %% add Node1 back to the set of candidates for cover plans,
    %% and turn YZ AAE back on.
    %%
    lager:info("Re-enabling dist_query on Node1 and turning on AAE ... "),
    rpc:call(Node1, yz_solr_proc, set_dist_query, [true]),
    rpc:call(Node1, application, set_env, [?YZ_APP_NAME, anti_entropy, {on, []}]),
    %%
    %% Wait for a full round of AAE
    %%
    yz_rt:wait_for_full_exchange_round(Cluster, erlang:now()),
    %%
    %% Verify that all missing Solr data has been repaired
    %% (Note that we never added more data to index-a1)
    %%
    verify_data(Cluster, ?INDEX_A1, 100),
    [verify_data(Cluster, Index, 200) ||
        Index <- [?INDEX_A2, ?INDEX_B1, ?INDEX_B2, ?INDEX_B3, ?INDEX_C]
    ],
    %%
    %% Write and verify the next 100 entries to all of our buckets,
    %% including the newly created index-c.
    %%
    write_and_verify_data(Cluster, ?BUCKET_A1, ?INDEX_A1, 101, 100, 200),
    rt:pmap(
        fun({Bucket, Index}) ->
            ok = write_and_verify_data(Cluster, Bucket, Index, 201, 100, 300)
        end,
        [{?BUCKET_A2, ?INDEX_A2},
         {?BUCKET_B1, ?INDEX_B1},
         {?BUCKET_B2, ?INDEX_B2},
         {?BUCKET_B3, ?INDEX_B3},
         {?BUCKET_C,  ?INDEX_C}]
    ),
    pass.

write_data(Cluster, Bucket, Start, End) ->
    ValueGenerator = fun(_Key) ->
        {"Same as it ever was", "text/plain"}
    end,
    lists:all(
        fun(Result) -> Result =:= ok end,
        yz_rt:write_data(Cluster, Bucket, Start, End, ValueGenerator)
    ).


verify_data(Cluster, Index, NumKeys) ->
    ok = yz_rt:verify_num_match(yokozuna, Cluster, Index, NumKeys),
    ExpectedSolrMatch = (?N_VAL * NumKeys * length(Cluster)) div ?CLUSTER_SIZE,
    ok = yz_rt:verify_num_match(solr, Cluster, Index, ExpectedSolrMatch),
    ok.

augment_config(ApplicationKey, Key, Value, Config) ->
    ApplicationConfig = proplists:get_value(ApplicationKey, Config),
    ApplicationDict = dict:from_list(ApplicationConfig),
    NewApplicationConfig = dict:to_list(dict:store(Key, Value, ApplicationDict)),
    lists:keyreplace(ApplicationKey, 1, Config, {ApplicationKey, NewApplicationConfig}).


update_yz(Params) ->
    NewDataDir = proplists:get_value(new_data_dir, Params),
    TimestampStr = timestamp_str(),
    %%
    %% Move yz_temp out of the way, because it contains cached Solr JAR files
    %% and other artifacts.
    %%
    ok = mv_yz_temp(NewDataDir, TimestampStr),
    %%
    %% Update the luceneMatchVersion in the Solr config
    %%
    [ok = modify_solr_config(NewDataDir, binary_to_list(Index),
        TimestampStr, ?LUCENE_MATCH_4_10_4_VERSION) ||
            Index <- [?INDEX_B1, ?INDEX_B2, ?INDEX_B3]
    ],
    %%
    %% Move the data directories for indices B2 and B3 out of the way
    %% (We will use the backup of B3 during downgrade)
    %%
    [mv_yz_index_data_dir(NewDataDir, binary_to_list(Index), TimestampStr)  ||
        Index <- [?INDEX_B2, ?INDEX_B3]],
    mv_yz_aae_dir(NewDataDir, TimestampStr),
    ok.

mv_yz_temp(DataDir, TimestampStr) ->
    lager:info("Moving yz_temp directory out of the way..."),
    YZTempPath = io_lib:format("~s/yz_temp", [DataDir]),
    ok = file:rename(YZTempPath, io_lib:format("~s-~s", [YZTempPath, TimestampStr])).

mv_yz_index_data_dir(DataDir, IndexName, TimestampStr) ->
    lager:info("Moving yz data directory for index ~s out of the way...", [IndexName]),
    YZTempPath = io_lib:format("~s/yz/~s/data", [DataDir, IndexName]),
    ok = file:rename(YZTempPath, io_lib:format("~s-~s", [YZTempPath, TimestampStr])).

mv_yz_aae_dir(DataDir, TimestampStr) ->
    lager:info("Moving yz AAE directory out of the way..."),
    YZTempPath = io_lib:format("~s/yz_anti_entropy", [DataDir]),
    ok = file:rename(YZTempPath, io_lib:format("~s-~s", [YZTempPath, TimestampStr])).

timestamp_str() ->
    Now = erlang:now(),
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:now_to_local_time(Now),
    io_lib:format("~b-~2..0b-~2..0bT~2..0b.~2..0b.~2..0b",
        [Year, Month, Day, Hour, Minute, Second]).

downgrade_yz(Params) ->
    NewDataDir = proplists:get_value(new_data_dir, Params),
    TimestampStr = timestamp_str(),
    %%
    %% Move yz_temp out of the way, because it contains cached Solr JAR files
    %% and other artifacts from the previous version that was copied by riak_test.
    %%
    ok = mv_yz_temp(NewDataDir, TimestampStr),
    %%
    %% move the yz_aae data out of the way
    %%
    mv_yz_aae_dir(NewDataDir, TimestampStr),
    ok.

finish_downgrade(Params) ->
    NewDataDir = proplists:get_value(new_data_dir, Params),
    TimestampStr = timestamp_str(),
    Indices =  [?INDEX_A2, ?INDEX_B1, ?INDEX_B2, ?INDEX_B3, ?INDEX_C],
    [modify_solr_config(
        NewDataDir, binary_to_list(Index), TimestampStr, ?LUCENE_MATCH_4_7_VERSION) ||
            Index <- Indices
    ],
    [mv_yz_index_data_dir(NewDataDir, binary_to_list(Index), TimestampStr)  ||
        Index <- Indices],
    ok.

modify_solr_config(NewDataDir, Index, TimestampStr, Version) ->
    lager:info("Changing solrconfig.xml for index ~s to version ~s ...",
        [Index, Version]),
    SolrConfigPath = io_lib:format(
        "~s/yz/~s/conf/solrconfig.xml", [NewDataDir, Index]
    ),
    SolrConfigBackupPath = io_lib:format(
        "~s-~s", [SolrConfigPath, TimestampStr]
    ),
    ok = file:rename(SolrConfigPath, SolrConfigBackupPath),
    {Doc, _Rest} = xmerl_scan:file(SolrConfigBackupPath),
    Doc2 = replace_version(Doc, Version),
    ExportIoList = xmerl:export_simple([Doc2], xmerl_xml),
    {ok, IOF} = file:open(SolrConfigPath,[write]),
    io:format(IOF, "~s",[ ExportIoList]),
    file:close(IOF),
    ok.

%%
%% @doc Replace the contents
%%
replace_version(#xmlElement{content = Contents} = Doc, Version) ->
    Doc#xmlElement{
        content = [replace_content(Content, Version) || Content <- Contents]
    }.

%%
%% @doc If a content is an XML Element with the tag `luceneMatchVersion` and
%% the content is a singleton with XML Text (which it should be), then
%% replace the value of the text with the specified version.
%% Otherwise, just leave it be.
%%
replace_content(
    #xmlElement{name = luceneMatchVersion,
        content = [#xmlText{} = Content]} = Element, Version) ->
    Element#xmlElement{
        content = [Content#xmlText{value = Version}]
    };
replace_content(Element, _Version) ->
    Element.

%%
%% Simple wrapper around yz_rt:create_indexed_bucket_type/4, because
%% the pmap callback only takes a tuple
%%
create_indexed_bucket_type({Cluster, BucketType, Index, BucketProperties}) ->
    ok = yz_rt:create_indexed_bucket_type(Cluster, BucketType, Index, BucketProperties).


%%
%% write and verify one one go
%%
write_and_verify_data(Cluster, Bucket, Index, Start, Num, Expected) ->
    true = write_data(Cluster, Bucket, Start, Num),
    ok = verify_data(Cluster, Index, Expected).
