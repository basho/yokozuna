%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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
-module(yz_core_properties_create_unload).
-compile(nowarn_export_all).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(CFG, [{riak_kv,
               [
                %% allow AAE to build trees and exchange rapidly
                {anti_entropy_build_limit, {1000, 1000}},
                {anti_entropy_concurrency, 64},
                {anti_entropy_tick, 1000}
               ]},
              {yokozuna,
               [
                {enabled, true},
                {anti_entropy_tick, 1000}
               ]}]).
-define(INDEX, <<"test_idx_core">>).
-define(TYPE, <<"data">>).
-define(BUCKET, {?TYPE, <<"test_bkt_core">>}).
-define(SEQMAX, 100).

confirm() ->
    Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),

    %% Generate keys, YZ only supports UTF-8 compatible keys
    Keys = [<<N:64/integer>> || N <- lists:seq(1, ?SEQMAX),
                               not lists:any(fun(E) -> E > 127 end,
                                             binary_to_list(<<N:64/integer>>))],
    KeyCount = length(Keys),

    %% Randomly select a subset of the test nodes to remove
    %% core.properties from
    RandNodes = rt:random_sublist(Cluster, 3),

    %% Select one of the modified nodes as a client endpoint
    Node = rt:select_random(RandNodes),
    Pid = rt:pbc(Node),
    riakc_pb_socket:set_options(Pid, [queue_if_disconnected]),

    %% Create a search index and associate with a bucket
    lager:info("Create and set Index ~p for Bucket ~p~n", [?INDEX, ?BUCKET]),
    _ = riakc_pb_socket:create_search_index(Pid, ?INDEX),
    yz_rt:wait_for_index(Cluster, ?INDEX),

    ok = rt:create_and_activate_bucket_type(Node,
                                            ?TYPE,
                                            [{search_index, ?INDEX}]),

    rt:wait_until_bucket_type_visible(Cluster, ?TYPE),

    %% Write keys and wait for soft commit
    lager:info("Writing ~p keys", [KeyCount]),
    [ok = rt:pbc_write(Pid, ?BUCKET, Key, Key, "text/plain") || Key <- Keys],
    yz_rt:commit(Cluster, ?INDEX),

    yz_rt:verify_num_found_query(Cluster, ?INDEX, KeyCount),


    test_core_props_removal(Cluster, RandNodes, KeyCount, Pid),
    test_remove_index_dirs(Cluster, RandNodes, KeyCount, Pid),
    test_remove_segment_infos_and_rebuild(Cluster, RandNodes, KeyCount, Pid),
    test_brutal_kill_and_delete_index_dirs(Cluster, RandNodes, KeyCount, Pid),

    riakc_pb_socket:stop(Pid),

    pass.

test_core_props_removal(Cluster, RandNodes, KeyCount, Pid) ->
    lager:info("Remove core.properties file in each index data dir"),
    remove_core_props(RandNodes, ?INDEX),

    yz_rt:check_exists(Cluster, ?INDEX),

    lager:info("Write one more piece of data"),
    ok = rt:pbc_write(Pid, ?BUCKET, <<"foo">>, <<"foo">>, "text/plain"),
    yz_rt:commit(Cluster, ?INDEX),

    yz_rt:verify_num_found_query(Cluster, ?INDEX, KeyCount+1).

test_remove_index_dirs(Cluster, RandNodes, KeyCount, Pid) ->
    lager:info("Remove index directories on each node and let them recreate/reindex"),
    yz_rt:remove_index_dirs(RandNodes, ?INDEX, [riak_kv, yokozuna]),

    yz_rt:check_exists(Cluster, ?INDEX),

    yz_rt:expire_trees(Cluster),
    yz_rt:wait_for_aae(Cluster),

    lager:info("Write second piece of data"),
    ok = rt:pbc_write(Pid, ?BUCKET, <<"food">>, <<"foody">>, "text/plain"),
    yz_rt:commit(Cluster, ?INDEX),

    yz_rt:verify_num_found_query(Cluster, ?INDEX, KeyCount+2).

test_remove_segment_infos_and_rebuild(Cluster, RandNodes, KeyCount, Pid) ->
    lager:info("Remove segment info files in each index data dir"),
    remove_segment_infos(RandNodes, ?INDEX),

    lager:info("To fix, we remove index directories on each node and let them recreate/reindex"),

    yz_rt:remove_index_dirs(RandNodes, ?INDEX, [riak_kv, yokozuna]),

    yz_rt:check_exists(Cluster, ?INDEX),

    yz_rt:expire_trees(Cluster),
    yz_rt:wait_for_aae(Cluster),

    lager:info("Write third piece of data"),
    ok = rt:pbc_write(Pid, ?BUCKET, <<"baz">>, <<"bar">>, "text/plain"),
    yz_rt:commit(Cluster, ?INDEX),

    yz_rt:verify_num_found_query(Cluster, ?INDEX, KeyCount+3).

test_brutal_kill_and_delete_index_dirs(Cluster, RandNodes, KeyCount, Pid) ->
    lager:info("Remove index directories on each node and let them recreate/reindex"),
    yz_rt:brutal_kill_remove_index_dirs(RandNodes, ?INDEX, [riak_kv, yokozuna]),

    yz_rt:check_exists(Cluster, ?INDEX),

    yz_rt:expire_trees(Cluster),
    yz_rt:wait_for_aae(Cluster),

    lager:info("Write fourth piece of data"),
    ok = rt:pbc_write(Pid, ?BUCKET, <<"food">>, <<"foody">>, "text/plain"),
    yz_rt:commit(Cluster, ?INDEX),

    yz_rt:verify_num_found_query(Cluster, ?INDEX, KeyCount + 4).

%% @doc Remove core properties file on nodes.
remove_core_props(Nodes, IndexName) ->
    IndexDirs = [rpc:call(Node, yz_index, index_dir, [IndexName]) ||
                    Node <- Nodes],
    PropsFiles = [filename:join([IndexDir, "core.properties"]) ||
                     IndexDir <- IndexDirs],
    lager:info("Remove core.properties files: ~p, on nodes: ~p~n",
               [PropsFiles, Nodes]),
    [file:delete(PropsFile) || PropsFile <- PropsFiles],
    ok.

%% @doc Remove lucence segment info files to check if reindexing will occur
%%      on re-creation/re-indexing.
remove_segment_infos(Nodes, IndexName) ->
    IndexDirs = [rpc:call(Node, yz_index, index_dir, [IndexName]) ||
                    Node <- Nodes],
    SiPaths = [binary_to_list(filename:join([IndexDir, "data/index/*.si"])) ||
                                     IndexDir <- IndexDirs],
    SiFiles = lists:append([filelib:wildcard(Path) || Path <- SiPaths]),
    lager:info("Remove segment info files: ~p, on in dirs: ~p~n",
               [SiFiles, IndexDirs]),
    [file:delete(SiFile) || SiFile <- SiFiles].
