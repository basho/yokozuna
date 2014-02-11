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

-record(index_info,
        {
          %% Each index has it's own N value. This is needed so that
          %% the query plan can be calculated. It is up to the user to
          %% make sure that all buckets associated with an index use
          %% the same N value as the index.
          n_val :: n(),

          %% The name of the schema this index is using.
          schema_name :: schema_name()
        }).
-type index_info() :: #index_info{}.

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
    case is_default_type_indexed(Index) of
        true -> [Index|Assoc];
        false -> Assoc
    end.

%% @see create/2
-spec create(index_name()) -> ok.
create(Name) ->
    create(Name, ?YZ_DEFAULT_SCHEMA_NAME).

%% @see create/3
-spec create(index_name(), schema_name()) -> ok | {error, schema_not_found}.
create(Name, SchemaName) ->
    DefaultNVal = riak_core_bucket:default_object_nval(),
    create(Name, SchemaName, DefaultNVal).

%% @doc Create the index `Name' across the entire cluster using
%%      `SchemaName' as the schema and `NVal' as the N value.
%%
%% `ok' - The schema was found and index added to the list.
%%
%% `schema_not_found' - The `SchemaName' could not be found.
-spec create(index_name(), schema_name(), n() | undefined) ->
                    ok |
                    {error, schema_not_found}.
create(Name, SchemaName, undefined) ->
    DefaultNVal = riak_core_bucket:default_object_nval(),
    create(Name, SchemaName, DefaultNVal);

create(Name, SchemaName, NVal) when is_integer(NVal),
                                    NVal > 0 ->
    case yz_schema:exists(SchemaName) of
        false ->
            {error, schema_not_found};
        true  ->
            Info = make_info(SchemaName, NVal),
            ok = riak_core_metadata:put(?YZ_META_INDEXES, Name, Info)
    end.

%% @doc Determine if an index exists. For an index to exist it must 1)
%% be written to official index list, 2) have a corresponding index
%% dir in the root dir and 3) respond to a ping indicating it started
%% properly.  If Solr is down then the check will fallback to
%% performing only the first two checks. If they fail then it
%% shouldn't exist in Solr.
-spec exists(index_name()) -> boolean().
exists(Name) ->
    InMeta = riak_core_metadata:get(?YZ_META_INDEXES, Name) /= undefined,
    DiskIndexNames = get_indexes_from_disk(?YZ_ROOT_DIR),
    OnDisk = lists:member(Name, DiskIndexNames),
    case yz_solr:is_up() of
        true ->
            InMeta andalso OnDisk andalso yz_solr:ping(Name);
        false ->
            InMeta andalso OnDisk
    end.

%% @doc Removed the index `Name' from cluster meta.
-spec remove(index_name()) -> ok.
remove(Name) ->
    ok = riak_core_metadata:delete(?YZ_META_INDEXES, Name).

%% @doc Determine list of indexes based on filesystem as opposed to
%% the Riak ring or Solr HTTP resource.
%%
%% NOTE: This function assumes that all Yokozuna indexes live directly
%% under the Yokozuna root data directory and that any dir with a
%% `core.properties' file is an index. DO NOT create a dir with a
%% `core.properties' for any other reason or it will confuse this
%% function and potentially have other consequences up the stack.
-spec get_indexes_from_disk(string()) -> [index_name()].
get_indexes_from_disk(Dir) ->
    Files = filelib:wildcard(filename:join([Dir, "*"])),
    [unicode:characters_to_binary(filename:basename(F))
     || F <- Files,
        filelib:is_dir(F) andalso
            filelib:is_file(filename:join([F, "core.properties"]))].

%% @doc Determine the list of indexes based on the cluster metadata.
-spec get_indexes_from_meta() -> indexes().
get_indexes_from_meta() ->
    riak_core_metadata:fold(fun meta_index_list_acc/2,
                            [], ?YZ_META_INDEXES, [{resolver, lww}]).

-spec get_index_info(index_name()) -> undefined | index_info().
get_index_info(Name) ->
    riak_core_metadata:get(?YZ_META_INDEXES, Name).

%% @doc Get the N value from the index info.
-spec get_n_val(index_info()) -> n().
get_n_val(IndexInfo) ->
    IndexInfo#index_info.n_val.

%% @doc Create the index `Name' locally.  Make best attempt to create
%%      the index, log if a failure occurs.  Always return `ok'.
%%
%% NOTE: This should typically be called by a the ring handler in
%%       `yz_event'.  The `create/1' API should be used to create a
%%       cluster-wide index.
-spec local_create(index_name()) -> ok.
local_create(Name) ->
    IndexDir = index_dir(Name),
    ConfDir = filename:join([IndexDir, "conf"]),
    ConfFiles = filelib:wildcard(filename:join([?YZ_PRIV, "conf", "*"])),
    DataDir = filename:join([IndexDir, "data"]),
    SchemaName = schema_name(get_index_info(Name)),
    case yz_schema:get(SchemaName) of
        {ok, RawSchema} ->
            SchemaFile = filename:join([ConfDir, yz_schema:filename(SchemaName)]),
            LocalSchemaFile = filename:join([".", yz_schema:filename(SchemaName)]),

            yz_misc:make_dirs([ConfDir, DataDir]),
            yz_misc:copy_files(ConfFiles, ConfDir, update),

            %% Delete `core.properties' file or CREATE may complain
            %% about the core already existing. This can happen when
            %% the core is initially created with a bad schema. Solr
            %% gets in a state where CREATE thinks the core already
            %% exists but RELOAD says no core exists.
            PropsFile = filename:join([IndexDir, "core.properties"]),
            file:delete(PropsFile),

            ok = file:write_file(SchemaFile, RawSchema),

            CoreProps = [
                         {name, Name},
                         {index_dir, IndexDir},
                         {cfg_file, ?YZ_CORE_CFG_FILE},
                         {schema_file, LocalSchemaFile}
                        ],
            case yz_solr:core(create, CoreProps) of
                {ok, _, _} ->
                    lager:info("Created index ~s with schema ~s",
                               [Name, SchemaName]),
                    ok;
                {error, Err} ->
                    lager:error("Couldn't create index ~s: ~p", [Name, Err])
            end,
            ok;
        {error, Reason} ->
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

%% @doc Reload the `Index' cluster-wide. By default this will also
%% pull the latest version of the schema associated with the
%% index. This call will block for up 5 seconds. Any node which could
%% not reload its index will be returned in a list of failed nodes.
%%
%% Options:
%%
%%   `{schema, boolean()}' - Whether to reload the schema, defaults to
%%   true.
%%
%%   `{timeout, ms()}' - Timeout in milliseconds.
-spec reload(index_name()) -> {ok, [node()]} | {error, reload_errs()}.
reload(Index) ->
    reload(Index, []).

-spec reload(index_name(), reload_opts()) -> {ok, [node()]} |
                                             {error, reload_errs()}.
reload(Index, Opts) ->
    TO = proplists:get_value(timeout, Opts, 5000),
    {Responses, Down} =
        riak_core_util:rpc_every_member_ann(?MODULE, reload_local, [Index, Opts], TO),
    Down2 = [{Node, {error,down}} || Node <- Down],
    BadResponses = [R || {_,{error,_}}=R <- Responses],
    case Down2 ++ BadResponses of
        [] ->
            Nodes = [Node || {Node,_} <- Responses],
            {ok, Nodes};
        Errors ->
            {error, Errors}
    end.

%% @doc Remove documents in `Index' that are not owned by the local
%%      node.  Return the list of non-owned partitions found.
-spec remove_non_owned_data(index_name(), ring()) -> [p()].
remove_non_owned_data(Index, Ring) ->
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

%% @private
%%
%% @doc Used to accumulate list of indexes while folding over index
%% metadata.
-spec meta_index_list_acc({index_name(), '$deleted' | term()}, indexes()) ->
                                 indexes().
meta_index_list_acc({_,'$deleted'}, Acc) ->
    Acc;
meta_index_list_acc({Key,_}, Acc) ->
    [Key|Acc].

%% @private
-spec reload_local(index_name(), reload_opts()) ->
                          ok | {error, term()}.
reload_local(Index, Opts) ->
    TO = proplists:get_value(timeout, Opts, 5000),
    ReloadSchema = proplists:get_value(schema, Opts, true),
    case ReloadSchema of
        true ->
            case reload_schema_local(Index) of
                ok ->
                    case yz_solr:core(reload, [{core, Index}], TO) of
                        {ok,_,_} -> ok;
                        Err -> Err
                    end;
                {error,_}=Err ->
                    Err
            end;
        false ->
            case yz_solr:core(reload, [{core, Index}]) of
                {ok,_,_} -> ok;
                Err -> Err
            end
    end.

%% @private
-spec reload_schema_local(index_name()) -> ok | {error, term()}.
reload_schema_local(Index) ->
    IndexDir = index_dir(Index),
    ConfDir = filename:join([IndexDir, "conf"]),
    SchemaName = schema_name(get_index_info(Index)),
    case yz_schema:get(SchemaName) of
        {ok, RawSchema} ->
            SchemaFile = filename:join([ConfDir, yz_schema:filename(SchemaName)]),
            file:write_file(SchemaFile, RawSchema);
        {error, Reason} ->
            {error, Reason}
    end.

index_dir(Name) ->
    filename:absname(filename:join([?YZ_ROOT_DIR, Name])).

%% @private
%%
%% @doc Determine if the bucket named `Index' under the default
%% bucket-type has `search' property set to `true'. If so this is a
%% legacy Riak Search bucket/index which is associated with a Yokozuna
%% index of the same name.
-spec is_default_type_indexed(index_name()) -> boolean().
is_default_type_indexed(Index) ->
    Props = riak_core_bucket:get_bucket(Index),
    %% Check against `true' atom in case the value is <<"true">> or
    %% "true" which, hopefully, it should not be.
    true == proplists:get_value(search, Props, false).

-spec make_info(binary(), n()) -> index_info().
make_info(SchemaName, NVal) ->
    #index_info{n_val=NVal,
                schema_name=SchemaName}.
