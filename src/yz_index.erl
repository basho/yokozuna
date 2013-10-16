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

%% @doc Get the list of buckets associated with `Index'.
-spec associated_buckets(index_name(), ring()) -> [bucket()].
associated_buckets(Index, Ring) ->
    AllProps = riak_core_bucket:get_buckets(Ring),
    Assoc = [riak_core_bucket:name(BProps)
             || BProps <- AllProps,
                proplists:get_value(?YZ_INDEX, BProps, ?YZ_INDEX_TOMBSTONE) == Index],
    case is_default_type_indexed(Index, Ring) of
        true -> [Index|Assoc];
        false -> Assoc
    end.

-spec create(index_name()) -> ok.
create(Name) ->
    create(Name, ?YZ_DEFAULT_SCHEMA_NAME).

%% @doc Create the index `Name' across the entire cluster using
%%      `SchemaName' as the schema.
%%
%% `ok' - The schema was found and the create request was successfully
%%        run on the claimant.
%%
%% `schema_not_found' - The `SchemaName' could not be found in Riak.
%%
%% `rpc_fail' - The claimant could not be contacted.
%%
%% NOTE: All create requests are serialized through the claimant node
%%       to avoid races between disjoint nodes.  If the claimant is
%%       down no indexes may be created.
-spec create(index_name(), schema_name()) -> ok |
                                             {error, schema_not_found} |
                                             {error, {rpc_fail, node(), term()}}.
create(Name, SchemaName) ->
    case yz_schema:exists(SchemaName) of
        false ->
            {error, schema_not_found};
        true  ->
            Ring = yz_misc:get_ring(transformed),
            case yz_misc:is_claimant(Ring, node()) of
                true ->
                    Info = make_info(Name, SchemaName),
                    ok = add_to_ring(Name, Info);
                false ->
                    Claimant = yz_misc:get_claimant(Ring),
                    case rpc:call(Claimant, ?MODULE, create, [Name, SchemaName]) of
                        ok ->
                            ok;
                        {badrpc, Reason} ->
                            {error, {rpc_fail, Claimant, Reason}}
                    end
            end
    end.

-spec exists(index_name()) -> boolean().
exists(Name) ->
    Indexes = get_indexes_from_ring(yz_misc:get_ring(raw)),
    InRing = orddict:is_key(Name, Indexes),
    SolrPing = yz_solr:ping(Name),
    InRing andalso SolrPing.


%% @doc Removed the index `Name' from the entire cluster.
-spec remove(index_name()) -> ok | {error, badrpc}.
remove(Name) ->
    Ring = yz_misc:get_ring(transformed),
    case yz_misc:is_claimant(Ring, node()) of
        true ->
            ok = remove_from_ring(Name);
        false ->
            case rpc:call(yz_misc:get_claimant(Ring), ?MODULE, remove, [Name]) of
                ok ->
                    ok;
                {badrpc, Reason} ->
                    lager:warning("Failed to contact claimant node ~p", [Reason]),
                    {error, badrpc}
            end
    end.

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

%% @doc Create the index `Name' locally.  Make best attempt to create
%%      the index, log if a failure occurs.  Always return `ok'.
%%
%% NOTE: This should typically be called by a the ring handler in
%%       `yz_event'.  The `create/1' API should be used to create a
%%       cluster-wide index.
-spec local_create(ring(), index_name()) -> ok.
local_create(Ring, Name) ->
    %% TODO: Allow data dir to be changed
    IndexDir = index_dir(Name),
    ConfDir = filename:join([IndexDir, "conf"]),
    ConfFiles = filelib:wildcard(filename:join([?YZ_PRIV, "conf", "*"])),
    DataDir = filename:join([IndexDir, "data"]),
    Info = get_info_from_ring(Ring, Name),
    SchemaName = schema_name(Info),
    case yz_schema:get(SchemaName) of
        {ok, RawSchema} ->
            SchemaFile = filename:join([ConfDir, yz_schema:filename(SchemaName)]),

            yz_misc:make_dirs([ConfDir, DataDir]),
            yz_misc:copy_files(ConfFiles, ConfDir, update),
            ok = file:write_file(SchemaFile, RawSchema),

            CoreProps = [
                         {name, Name},
                         {index_dir, IndexDir},
                         {cfg_file, ?YZ_CORE_CFG_FILE},
                         {schema_file, SchemaFile}
                        ],
            case yz_solr:core(create, CoreProps) of
                {ok, _, _} ->
                    ok;
                {error, Err} ->
                    lager:error("Couldn't create index ~s: ~p", [Name, Err])
            end,
            ok;
        {error, _, Reason} ->
            lager:error("Couldn't create index ~s: ~p", [Name, Reason]),
            ok
    end.

%% @doc Remove the index `Name' locally.
-spec local_remove(index_name()) -> ok.
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
-spec remove_non_owned_data(index_name()) -> [p()].
remove_non_owned_data(Index) ->
    Ring = yz_misc:get_ring(raw),
    IndexPartitions = yz_cover:reify_partitions(Ring,
                                                yokozuna:partition_list(Index)),
    OwnedAndNext = yz_misc:owned_and_next_partitions(node(), Ring),
    NonOwned = ordsets:subtract(IndexPartitions, OwnedAndNext),
    LNonOwned = yz_cover:logical_partitions(Ring, NonOwned),
    Queries = [{'query', <<?YZ_PN_FIELD_S, ":", (?INT_TO_BIN(LP))/binary>>}
               || LP <- LNonOwned],
    ok = yz_solr:delete(Index, Queries),
    NonOwned.

-spec schema_name(index_info()) -> schema_name().
schema_name(Info) ->
    Info#index_info.schema_name.

%%%===================================================================
%%% Private
%%%===================================================================

-spec add_index(indexes(), {index_name(), index_info()}) -> indexes().
add_index(Indexes, {Name, Info}) ->
    orddict:store(Name, Info, Indexes).

-spec add_to_ring(index_name(), index_info()) -> ok.
add_to_ring(Name, Info) ->
    %% checking return value, just to guard against surprises in
    %% future API changes
    case yz_misc:set_ring_meta(
           ?YZ_META_INDEXES, [], fun add_index/2, {Name, Info}) of
        {ok, _} ->
            ok;
        not_changed ->
            %% index existed already
            ok
    end.

-spec remove_index(indexes(), index_name()) -> indexes().
remove_index(Indexes, Name) ->
    orddict:erase(Name, Indexes).

-spec remove_from_ring(index_name()) -> ok.
remove_from_ring(Name) ->
    %% checking return value, just to guard against surprises in
    %% future API changes
    case yz_misc:set_ring_meta(
           ?YZ_META_INDEXES, [], fun remove_index/2, Name) of
        {ok, _} ->
            ok;
        not_changed ->
            %% index did not exist
            ok
    end.

index_dir(Name) ->
    YZDir = app_helper:get_env(?YZ_APP_NAME, yz_dir, ?YZ_DEFAULT_DIR),
    filename:absname(filename:join([YZDir, Name])).

%% @private
%%
%% @doc Determine if the bucket named `Index' under the default
%% bucket-type has `search' property set to `true'. If so41 this is a
%% legacy Riak Search bucket/index which is associated with a Yokozuna
%% index of the same name.
-spec is_default_type_indexed(index_name(), ring()) -> boolean().
is_default_type_indexed(Index, Ring) ->
    Props = riak_core_bucket:get_bucket(Index, Ring),
    %% Check against `true' atom in case the value is <<"true">> or
    %% "true" which, hopefully, it should not be.
    true == proplists:get_value(search, Props, false).

make_info(IndexName, SchemaName) ->
    #index_info{name=IndexName,
                schema_name=SchemaName}.
