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

-spec add_to_ring(string()) -> ok.
add_to_ring(Name) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    ok = add_to_ring(Ring, Name).

%% TODO: Allow data dir to be changed
-spec create(string()) -> ok.
create(Name) ->
    IndexDir = index_dir(Name),
    ConfDir = filename:join([IndexDir, "conf"]),
    ConfFiles = filelib:wildcard(filename:join([?YZ_PRIV, "conf", "*"])),
    DataDir = filename:join([IndexDir, "data"]),

    make_dirs([ConfDir, DataDir]),
    copy_files(ConfFiles, ConfDir),

    CoreProps = [
                 {name, Name},
                 {index_dir, IndexDir},
                 {cfg_file, ?YZ_CORE_CFG_FILE},
                 {schema_file, ?YZ_SCHEMA_FILE}
                ],
    {ok, _, _} = yz_solr:core(create, CoreProps),
    ok.

-spec exists(string()) -> boolean().
exists(Name) ->
    true == yz_solr:ping(Name).

-spec get_indexes_from_ring(ring()) -> ordset(p()).
get_indexes_from_ring(Ring) ->
    case riak_core_ring:get_meta(?YZ_META_INDEXES, Ring) of
        {ok, Indexes} -> Indexes;
        undefined -> []
    end.

-spec indexes() -> ordset(index_name()).
indexes() ->
    {ok, _, Body} = yz_solr:core(status, [{wt,json}]),
    Status = yz_solr:get_path(mochijson2:decode(Body), [<<"status">>]),
    ordsets:from_list([binary_to_list(Name) || {Name, _} <- Status]).

%% @doc Remove documents in `Index' that are not owned by the local
%%      node.  Return the list of non-owned partitions found.
-spec remove_non_owned_data(string()) -> list().
remove_non_owned_data(Index) ->
    IndexPartitions = yokozuna:partition_list(Index),
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    OwnedAndNext = yz_misc:owned_and_next_partitions(node(), Ring),
    NonOwned = ordsets:subtract(IndexPartitions, OwnedAndNext),
    Query = yz_solr:build_partition_delete_query(NonOwned),
    ok = yz_solr:delete_by_query(Index, Query),
    NonOwned.

%%%===================================================================
%%% Private
%%%===================================================================

add_to_ring(Ring, Name) ->
    Indexes = get_indexes_from_ring(Ring),
    Indexes2 = ordsets:add_element(Name, Indexes),
    Ring2 = riak_core_ring:update_meta(?YZ_META_INDEXES, Indexes2, Ring),
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

schema_file() ->
    ?YZ_PRIV ++ "/" ++ ?YZ_SCHEMA_FILE.

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
