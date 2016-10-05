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

%% @doc Confirm that fuses are created for Solr indexes across upgrades.

-module(yz_fuse_upgrade).
-export([confirm/0]).

-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(OLD_BUCKET, <<"old_bucket">>).
-define(OLD_INDEX, <<"old_index">>).
-define(NEW_BUCKET, <<"new_bucket">>).
-define(NEW_INDEX, <<"new_index">>).
-define(CLUSTER_SIZE, 2).
-define(CONFIG,
        [{riak_core,
          [{ring_creation_size, 16}]},
         {yokozuna,
          [{enabled, true}]}
        ]).

confirm() ->
    %% Fixing to a "pre-batching/fuse" version, this can be version <= 2.0.6.
    OldVsn = "2.0.5",

    Cluster = rt:build_cluster(lists:duplicate(?CLUSTER_SIZE,
                                               {OldVsn, ?CONFIG})),
    Node1 = hd(Cluster),

    yz_rt:create_index(Cluster, ?OLD_INDEX),
    yz_rt:set_index(Node1, ?OLD_BUCKET, ?OLD_INDEX),

    yz_rt:rolling_upgrade(Cluster, current, ?CONFIG, [riak_kv, yokozuna]),

    yz_rt:create_index(Cluster, ?NEW_INDEX),
    yz_rt:set_index(Node1, ?NEW_BUCKET, ?NEW_INDEX),

    ?assertEqual(ok,
                 yz_rt:wait_until(Cluster, fun verify_fuse_for_old_index/1)),
    ?assertEqual(ok,
                 yz_rt:wait_until(Cluster, fun verify_fuse_for_new_index/1)),

    pass.

verify_fuse_for_old_index(Node) ->
    Result = rpc:call(Node, yz_fuse, check, [?OLD_INDEX]),
    lager:info("Fuse Check Old Index ~p", [Result]),
    ok =:= Result.

verify_fuse_for_new_index(Node) ->
    Result = rpc:call(Node, yz_fuse, check, [?NEW_INDEX]),
    lager:info("Fuse Check New Index ~p", [Result]),
    ok =:= Result.
