%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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
%% -------------------------------------------------------------------

-module(yz_index).
-include("yokozuna.hrl").
-compile(export_all).

-define(YZ_DEFAULT_DIR, filename:join(["data", "yz"])).

%% @doc This module contains functionaity for using and administrating
%%      indexes.  In this case an index is an instance of a Solr Core.

%%%===================================================================
%%% API
%%%===================================================================

-spec create(string()) -> ok.
create(Name) ->
    create(Name, ?YZ_DEFAULT_SCHEMA_NAME).

%% @doc Create the index `Name' across the entire cluster using
%%      `SchemaName' as the schema.
%%      Returns 'ok' if created, or 'notfound' if the
%%      `SchemaName' is unknown
-spec create(string(), schema_name()) -> ok | notfound.
create(Name, SchemaName) ->
    case yz_schema:exists(SchemaName) of
        false -> notfound;
        true  ->
            Info = make_info(Name, SchemaName),
            ok = add_to_ring(Name, Info)
    end.

-spec exists(string()) -> boolean().
exists(Name) ->
    true == yz_solr:ping(Name).

%% @doc Removed the index `Name' from the entire cluster.
-spec remove(string()) -> ok.
remove(Name) ->
    ok = remove_from_ring(Name).

-spec get_indexes_from_ring(ring()) -> indexes().
get_indexes_from_ring(Ring) ->
    case riak_core_ring:get_meta(?YZ_META_INDEXES, Ring) of
        {ok, Indexes} -> Indexes;
        undefined -> []
    end.

-spec get_info_from_ring(ring(), index_name()) -> index_info().
get_info_from_ring(Ring, Name) ->
    Indexes = get_indexes_from_ring(Ring),
    orddict:fetch(Name, Indexes).

%% @doc Create the index `Name' locally.
%%
%% NOTE: This should typically be called by a the ring handler in
%%       `yz_event'.  The `create/1' API should be used to create a
%%       cluster-wide index.
-spec local_create(ring(), string()) -> ok.
local_create(Ring, Name) ->
    %% TODO: Allow data dir to be changed
    IndexDir = index_dir(Name),
    ConfDir = filename:join([IndexDir, "conf"]),
    ConfFiles = filelib:wildcard(filename:join([?YZ_PRIV, "conf", "*"])),
    DataDir = filename:join([IndexDir, "data"]),
    Info = get_info_from_ring(Ring, Name),
    SchemaName = schema_name(Info),
    RawSchema = yz_schema:get(SchemaName),
    SchemaFile = filename:join([ConfDir, yz_schema:filename(SchemaName)]),

    yz_misc:make_dirs([ConfDir, DataDir]),
    yz_misc:copy_files(ConfFiles, ConfDir),
    ok = file:write_file(SchemaFile, RawSchema),

    CoreProps = [
                 {name, Name},
                 {index_dir, IndexDir},
                 {cfg_file, ?YZ_CORE_CFG_FILE},
                 {schema_file, SchemaFile}
                ],
    {ok, _, _} = yz_solr:core(create, CoreProps),
    ok.

%% @doc Remove the index `Name' locally.
-spec local_remove(string()) -> ok.
local_remove(Name) ->
    CoreProps = [
                    {core, Name},
                    {delete_instance, "true"}
                ],
    {ok, _, _} = yz_solr:core(remove, CoreProps),
    ok.

name(Info) ->
    Info#index_info.name.

%% @doc Remove documents in `Index' that are not owned by the local
%%      node.  Return the list of non-owned partitions found.
-spec remove_non_owned_data(string()) -> [{ordset(p()), ordset(lp())}].
remove_non_owned_data(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    IndexPartitions = yz_cover:reify_partitions(Ring,
                                                yokozuna:partition_list(Index)),
    OwnedAndNext = yz_misc:owned_and_next_partitions(node(), Ring),
    NonOwned = ordsets:subtract(IndexPartitions, OwnedAndNext),
    LNonOwned = yz_cover:logical_partitions(Ring, NonOwned),
    Query = yz_solr:build_partition_delete_query(LNonOwned),
    ok = yz_solr:delete_by_query(Index, Query),
    lists:zip(NonOwned, LNonOwned).

-spec schema_name(index_info()) -> schema_name().
schema_name(Info) ->
    Info#index_info.schema_name.

%%%===================================================================
%%% Private
%%%===================================================================

-spec add_index(indexes(), index_name(), index_info()) -> indexes().
add_index(Indexes, Name, Info) ->
    orddict:store(Name, Info, Indexes).

-spec add_to_ring(index_name(), index_info()) -> ok.
add_to_ring(Name, Info) ->
    Indexes = get_indexes_from_ring(yz_misc:get_ring(transformed)),
    Indexes2 = add_index(Indexes, Name, Info),
    yz_misc:set_ring_meta(?YZ_META_INDEXES, Indexes2),
    ok.

-spec remove_index(indexes(), index_name()) -> indexes().
remove_index(Indexes, Name) ->
    orddict:erase(Name, Indexes).

-spec remove_from_ring(index_name()) -> ok.
remove_from_ring(Name) ->
    Indexes = get_indexes_from_ring(yz_misc:get_ring(transformed)),
    Indexes2 = remove_index(Indexes, Name),
    yz_misc:set_ring_meta(?YZ_META_INDEXES, Indexes2),
    ok.

index_dir(Name) ->
    YZDir = app_helper:get_env(?YZ_APP_NAME, yz_dir, ?YZ_DEFAULT_DIR),
    filename:absname(filename:join([YZDir, Name])).

make_info(IndexName, SchemaName) ->
    #index_info{name=IndexName,
                schema_name=SchemaName}.
