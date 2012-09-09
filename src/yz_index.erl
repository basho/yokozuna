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
-spec create(string(), schema_name()) -> ok.
create(Name, SchemaName) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Info = make_info(Name, SchemaName),
    ok = add_to_ring(Ring, Name, Info).

-spec exists(string()) -> boolean().
exists(Name) ->
    true == yz_solr:ping(Name).

-spec get_indexes_from_ring(ring()) -> indexes().
get_indexes_from_ring(Ring) ->
    case riak_core_ring:get_meta(?YZ_META_INDEXES, Ring) of
        {ok, Indexes} -> Indexes;
        undefined -> []
    end.

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

    make_dirs([ConfDir, DataDir]),
    copy_files(ConfFiles, ConfDir),
    ok = file:write_file(SchemaFile, RawSchema),

    CoreProps = [
                 {name, Name},
                 {index_dir, IndexDir},
                 {cfg_file, ?YZ_CORE_CFG_FILE},
                 {schema_file, SchemaFile}
                ],
    {ok, _, _} = yz_solr:core(create, CoreProps),
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

-spec add_to_ring(ring(), index_name(), index_info()) -> ok.
add_to_ring(Ring, Name, Info) ->
    Indexes = get_indexes_from_ring(Ring),
    Indexes2 = add_index(Indexes, Name, Info),
    Ring2 = riak_core_ring:update_meta(?YZ_META_INDEXES, Indexes2, Ring),
    %% TODO Is the ring trans needed or does update_meta already do
    %%      that for me?
    {ok, _Ring3} = riak_core_ring_manager:ring_trans(set_ring_trans(Ring2), []),
    ok.

copy_files([], _) ->
    ok;
copy_files([File|Rest], Dir) ->
    Basename = filename:basename(File),
    case filelib:is_dir(File) of
        true ->
            NewDir = filename:join([Dir, Basename]),
            make_dir(NewDir),
            copy_files(filelib:wildcard(filename:join([File, "*"])), NewDir),
            copy_files(Rest, Dir);
        false ->
            {ok, _} = file:copy(File, filename:join([Dir, Basename])),
            copy_files(Rest, Dir)
    end.

index_dir(Name) ->
    YZDir = app_helper:get_env(?YZ_APP_NAME, yz_dir, ?YZ_DEFAULT_DIR),
    filename:absname(filename:join([YZDir, Name])).

make_dir(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            ok;
        false ->
            ok = filelib:ensure_dir(Dir),
            ok = file:make_dir(Dir)
    end.

make_dirs([]) ->
    ok;
make_dirs([Dir|Rest]) ->
    ok = make_dir(Dir),
    make_dirs(Rest).

make_info(IndexName, SchemaName) ->
    #index_info{name=IndexName,
                schema_name=SchemaName}.

set_ring_trans(Ring) ->
    fun(_,_) -> {new_ring, Ring} end.


%% Old stuff relate to exception handling ideas I was playing with

%% create2(Name, _Schema) ->
%%     CfgDir = cfg_dir(Name),

%%     try
%%         ok = make_dir(CfgDir)
%%         %% ok = copy_config(CfgDir),
%%         %% ok = copy_schema(Schema, CfgDir)
%%     catch _:Reason ->
%%             %% If file calls don't throw then reason will be badmatch,
%%             %% if they throw something like {"cannot make dir",
%%             %% Reason} then it's more clear.
%%             ?ERROR("Problem creating index ~p ~p~n", [Name, Reason]),
%%             %% TODO: this will return `ok` I should actually be
%%             %% throwing here too and building up the exception
%%             %% chain...the key is determining the boundary of when to
%%             %% stop...perhaps the function at stop of stack for a
%%             %% process?
%%     end.

%% create_dir(Name) ->
%%     CfgDir = cfg_dir(Name),
%%     case filelib:ensure_dir(CfgDir) of
%%         ok ->
%%             case file:make_dir(CfgDir) of
%%                 ok ->
%%                     {ok, CfgDir};
%%                 Err ->
%%                     Err
%%             end;
%%         Err ->
%%             Err
%%     end.

%% make_dir(Dir) ->
%%     try
%%         ok = filelib:ensure_dir(Dir),
%%         ok = file:make_dir(Dir)
%%     catch error:Reason ->
%%             throw({error_creating_dir, Dir, Reason})
%%     end.
