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

%% @doc This module contains functionaity for using and administrating
%%      indexes.  In this case an index is an instance of a Solr Core.

-module(yz_index).
-include("yokozuna.hrl").
-compile(export_all).

-define(SOLR_INITFAILURES(I, S), kvc:path(erlang:iolist_to_binary(
                                            [<<"initFailures">>, <<".">>, I]),
                                          mochijson2:decode(S))).

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


%%%===================================================================
%%% API
%%%===================================================================

%% @doc Get the list of buckets associated with `Index'.
-spec associated_buckets(index_name(), ring()) -> [bucket()].
associated_buckets(Index, Ring) ->
    AllProps = riak_core_bucket:get_buckets(Ring),
    [riak_core_bucket:name(BProps)
     || BProps <- AllProps,
        proplists:get_value(?YZ_INDEX, BProps, ?YZ_INDEX_TOMBSTONE) == Index].

%% @see create/2
-spec create(index_name()) -> ok.
create(Name) ->
    create(Name, ?YZ_DEFAULT_SCHEMA_NAME).

%% @see create/3
-spec create(index_name(), schema_name()) ->
                    ok |
                    {error, schema_not_found} |
                    {error, invalid_name}.
create(Name, SchemaName) ->
    DefaultNVal = riak_core_bucket:default_object_nval(),
    create(Name, SchemaName, DefaultNVal).

%% @doc Create the index `Name' across the entire cluster using
%%      `SchemaName' as the schema and `NVal' as the N value.
%%
%% `ok' - The schema was found and index added to the list.
%%
%% `schema_not_found' - The `SchemaName' could not be found.
%% @see create/4
-spec create(index_name(), schema_name(), n() | undefined) ->
                    ok |
                    {error, index_not_created_within_timeout} |
                    {error, schema_not_found} |
                    {error, invalid_name} |
                    {error, core_error_on_index_creation, binary()}.
create(Name, SchemaName, undefined) ->
    DefaultNVal = riak_core_bucket:default_object_nval(),
    create(Name, SchemaName, DefaultNVal, ?DEFAULT_IDX_CREATE_TIMEOUT);
create(Name, SchemaName, NVal) ->
    create(Name, SchemaName, NVal, ?DEFAULT_IDX_CREATE_TIMEOUT).

-spec create(index_name(), schema_name(), n() | undefined, timeout()) ->
                    ok |
                    {error, index_not_created_within_timeout} |
                    {error, schema_not_found} |
                    {error, invalid_name} |
                    {error, core_error_on_index_creation, binary()}.
create(Name, SchemaName, undefined, Timeout) ->
    DefaultNVal = riak_core_bucket:default_object_nval(),
    create(Name, SchemaName, DefaultNVal, Timeout);
create(Name, SchemaName, NVal, Timeout) when is_integer(NVal),
                                             NVal > 0 ->
    case verify_name(Name) of
        {ok, Name} ->
            case yz_schema:exists(SchemaName) of
                false ->
                    {error, schema_not_found};
                true  ->
                    Info = make_info(SchemaName, NVal),
                    %% Propagate index across cluster
                    ok = riak_core_metadata:put(?YZ_META_INDEXES, Name, Info),

                    %% Spawn a process that spawns a linked child process that
                    %% waits for the index to exist within the set timeout.

                    %% Propagation of the index can still occur, but may
                    %% take longer than the alloted timeout for the request.
                    spawn(?MODULE, sync_index, [self(), Name, Timeout]),
                    receive
                        ok ->
                            ok;
                        {core_error, Error} ->
                            {error, core_error_on_index_creation, Error};
                        timeout ->
                            {error, index_not_created_within_timeout}
                    end
            end;
        {error, _} = Err ->
            Err
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
                {error, exists} ->
                    lager:info("Index ~s already exists in Solr, "
                               "but not in Riak metadata",
                               [Name]);
                {error, Err} ->
                    lager:error("Couldn't create index ~s: ~p", [Name, Err])
            end,
            ok;
        {error, _Reason} ->
            lager:error("Couldn't create index ~s because the schema ~s isn't found",
                        [Name, SchemaName]),
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

%% @doc Verify that the index is a name that Solr can use. Some chars
%%      are invalid, namely "/" or non-ascii characters until full
%%      UTF-8 support is available
-spec verify_name(index_name()) -> {ok, index_name()} | {error, invalid_name}.
verify_name(Name) ->
    case lists:dropwhile(fun(X) -> X < 128 andalso X > 31 end,
                         binary_to_list(Name)) =:= "" of
        true ->
            case re:run(Name, "/", []) of
                nomatch ->   {ok, Name};
                {match,_} -> {error, invalid_name}
            end;
        false ->
            {error, invalid_name}
    end.

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

-spec make_info(binary(), n()) -> index_info().
make_info(SchemaName, NVal) ->
    #index_info{n_val=NVal,
                schema_name=SchemaName}.

-spec sync_index(pid(), index_name(), timeout()) -> ok | timeout |
                                                   {core_error, binary()}.
sync_index(Pid, IndexName, Timeout) ->
    process_flag(trap_exit, true),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Nodes = riak_core_ring:all_members(Ring),
    WaitPid = spawn_link(?MODULE, wait_for_index, [self(), IndexName, Nodes]),
    receive
        {_From, ok} ->
            Pid ! ok;
        {'EXIT', _Pid, _Reason} ->
            sync_index(Pid, IndexName, Timeout)
    after Timeout ->
            exit(WaitPid, kill),

            %% Check if initFailure occurred
            {ok, _, S} = yz_solr:core(status, [{wt,json},{core, IndexName}]),
            case ?SOLR_INITFAILURES(IndexName, S) of
                [] ->
                    lager:notice("Index ~s not created within ~p ms timeout",
                                  [IndexName, Timeout]),
                    Pid ! timeout;
                Error ->
                    lager:error("Solr core error after trying to create index ~s: ~p",
                                [IndexName, Error]),
                    Pid ! {core_error, Error}
            end
    end.

-spec wait_for_index(pid(), index_name(), [Node :: term()]) -> {pid(), ok}.
wait_for_index(Pid, IndexName, Nodes) ->
    Results =  riak_core_util:multi_rpc_ann(Nodes, ?MODULE, exists, [IndexName]),
    case lists:filter(fun({_, Res}) -> Res =/= true end, Results) of
        [] ->
            Pid ! {self(), ok};
        Rest ->
            wait_for_index(Pid, IndexName, [Node || {Node, _} <- Rest])
    end.
