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
%%--------------------------------------------------------------------

%% @doc Test that checks to make sure that default bucket_types
%%      do not lose data when expiring/clearing AAE trees when
%%      trees are rebuilt for comparison.
%% @end


-module(yz_default_bucket_type_upgrade).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(N, 3).
-define(YZ_CAP, {yokozuna, handle_legacy_default_bucket_type_aae}).
-define(INDEX, <<"test_upgrade_idx">>).
-define(BUCKET, <<"test_upgrade_bucket">>).
-define(SEQMAX, 2000).
-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 16},
           {default_bucket_props,
               [
                   {n_val, ?N},
                   {allow_mult, true},
                   {dvv_enabled, true}
               ]}
          ]},
         {riak_kv,
             [
                 {anti_entropy_build_limit, {100, 1000}},
                 {anti_entropy_concurrency, 8}
             ]
         },
         {yokozuna,
          [
           {anti_entropy_tick, 1000},
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

    GenKeys = yz_rt:gen_keys(?SEQMAX),
    KeyCount = length(GenKeys),
    lager:info("KeyCount ~p", [KeyCount]),

    OldPid = rt:pbc(Node),

    yz_rt:pb_write_data(Cluster, OldPid, ?INDEX, ?BUCKET, GenKeys),
    yz_rt:commit(Cluster, ?INDEX),

    yz_rt:verify_num_found_query(Cluster, ?INDEX, KeyCount),

    %% Upgrade
    yz_rt:rolling_upgrade(Cluster, current),

    [rt:assert_capability(ANode, ?YZ_CAP, v1) || ANode <- Cluster],
    [rt:assert_supported(rt:capability(ANode, all), ?YZ_CAP, [v1, v0]) || ANode <- Cluster],

    yz_rt:verify_num_found_query(Cluster, ?INDEX, KeyCount),

    lager:info("Write one more piece of data"),
    Pid = rt:pbc(Node),
    ok = rt:pbc_write(Pid, ?BUCKET, <<"foo">>, <<"foo">>, "text/plain"),
    yz_rt:commit(Cluster, ?INDEX),

    yz_rt:expire_trees(Cluster),
    yz_rt:verify_num_found_query(Cluster, ?INDEX, KeyCount + 1),

    pass.
