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
%%
%% @doc This module contains functionality related to integrating with
%%      Riak KV.

-module(yz_kv).
-compile(export_all).
-include_lib("riak_core/include/riak_core_bucket_type.hrl").
-include("yokozuna.hrl").

-type write_reason() :: delete | handoff | put | anti_entropy.


%%%===================================================================
%%% TODO: move to riak_core
%%%===================================================================

bucket_name({_,Name}) ->
    Name;
bucket_name(Name) ->
    Name.

bucket_type({Type,_}) ->
    Type;
bucket_type(_) ->
    ?DEFAULT_TYPE.

is_default_type({?DEFAULT_TYPE,_}) ->
    true;
is_default_type({_,_}) ->
    false;
is_default_type(_) ->
    true.


%%%===================================================================
%%% API
%%%===================================================================

all_exchanges(Ring, Index) ->
    {Index, yz_entropy_mgr:all_pairwise_exchanges(Index, Ring)}.

-spec client() -> any().
client() ->
    {ok,C} = riak:local_client(),
    C.

%% @doc Compute the AAE exchange info.
-spec compute_exchange_info() -> [{p(), timestamp(), timestamp(), term()}].
compute_exchange_info() ->
    riak_kv_entropy_info:compute_exchange_info(yz, {?MODULE, all_exchanges}).

%% @doc Compute the AAE hashtree info.
-spec compute_tree_info() -> [{p(), timestamp()}].
compute_tree_info() ->
    riak_kv_entropy_info:compute_tree_info(yz).

-spec get(any(), binary(), binary()) -> any().
get(C, Bucket, Key) ->
    case C:get(Bucket, Key) of
        {ok, O} ->
            {value, riak_object:get_value(O)};
        Other ->
            Other
    end.

%% @doc calculates the hash of a riak object, returns binary 
-spec hash_object(riak_object:riak_object()) -> binary().
hash_object(Obj) ->
    Vclock = riak_object:vclock(Obj),
    Obj2 = riak_object:set_vclock(Obj, lists:sort(Vclock)),
    Hash = erlang:phash2(term_to_binary(Obj2)),
    term_to_binary(Hash).

%% @doc Get the content-type of the object.
-spec get_obj_ct(obj_metadata()) -> binary().
get_obj_ct(MD) ->
    dict:fetch(<<"content-type">>, MD).

-spec get_obj_bucket(obj()) -> bucket().
get_obj_bucket(Obj) ->
    riak_object:bucket(Obj).

-spec get_obj_key(obj()) -> binary().
get_obj_key(Obj) ->
    riak_object:key(Obj).

-spec get_obj_md(obj()) -> undefined | dict().
get_obj_md(Obj) ->
    riak_object:get_metadata(Obj).

-spec get_obj_value(obj()) -> binary().
get_obj_value(Obj) ->
    riak_object:get_value(Obj).

%% @doc Get the build time of the tree.
-spec get_tree_build_time(hashtree:hashtree()) -> any().
get_tree_build_time(Tree) ->
    riak_kv_index_hashtree:get_build_time(Tree).

%% @doc Determine if the `Obj' is a tombstone.
-spec is_tombstone(obj_metadata()) -> boolean().
is_tombstone(MD) ->
    case yz_misc:dict_get(<<"X-Riak-Deleted">>, MD, false) of
        "true" -> true;
        false -> false
    end.

get_md_entry(MD, Key) ->
    yz_misc:dict_get(Key, MD, none).

%% @doc Extract the index name from the `Bucket'. Return the tombstone
%% value if there is none.
-spec get_index(bkey()) -> index_name().
get_index({Bucket, _}) ->
    BProps = riak_core_bucket:get_bucket(Bucket),
    proplists:get_value(?YZ_INDEX, BProps, ?YZ_INDEX_TOMBSTONE).

%% @doc Called by KV vnode to determine if handoff should start or
%% not.  Yokozuna needs to make sure that the bucket types have been
%% transfered first.  Otherwise the bucket-to-index associations may
%% be missing causing missing index entries.
%%
%% TODO: Currently this call will block vnode and also vnode mgr.  If
%% I want to get really fancy the first time this function is called I
%% could return false but then send off async job to wait for bucket
%% types to transfer.  Once types have transfered some global flag
%% which is cheap to check would be set and this call would simply
%% check that.
-spec should_handoff({term(), {p(), node()}}) -> boolean().
should_handoff({_Reason, {_Partition, TargetNode}}) ->
    case ?YZ_ENABLED andalso is_service_up(?YZ_SVC_NAME, TargetNode) of
        true ->
            case is_metadata_consistent(TargetNode) andalso
                 has_indexes(TargetNode) of
                true ->
                    true;
                false ->
                    ?INFO("waiting for bucket types prefix and indexes to agree between ~p and ~p",
                          [node(), TargetNode]),
                    false
            end;
        false ->
            true
    end.

%% @doc Returns true if the RemoteNode's indexes
%% match this node's indexes.
-spec has_indexes(node()) -> boolean().
has_indexes(RemoteNode) ->
    RemoteIndexes = case rpc:call(RemoteNode, yz_solr, cores, [], 5000) of
        {ok, Indexes} -> Indexes;
        _ -> error
    end,
    case {RemoteIndexes, yz_solr:cores()} of
        {error, _} -> false;
        {RemoteIndexes, {ok, LocalIndexes}} ->
            lists:sort(LocalIndexes) == lists:sort(RemoteIndexes);
        _ -> false
    end.

index(Obj, Reason, P) ->
    case yokozuna:is_enabled(index) andalso ?YZ_ENABLED of
        true ->
            Ring = yz_misc:get_ring(transformed),
            case is_owner_or_future_owner(P, node(), Ring) of
                true ->
                    T1 = os:timestamp(),
                    BKey = {riak_object:bucket(Obj), riak_object:key(Obj)},
                    try
                        Index = get_index(BKey),
                        ShortPL = riak_kv_util:get_index_n(BKey),
                        case should_index(Index) of
                            true ->
                                index(Obj, Reason, Ring, P, BKey, ShortPL, Index);
                            false ->
                                dont_index(Obj, Reason, P, BKey, ShortPL)
                        end,
                        yz_stat:index_end(?YZ_TIME_ELAPSED(T1))
                    catch _:Err ->
                            yz_stat:index_fail(),
                            Trace = erlang:get_stacktrace(),
                            case Reason of
                                delete ->
                                    ?ERROR("failed to delete docid ~p with error ~p because ~p",
                                           [BKey, Err, Trace]);
                                _ ->
                                    ?ERROR("failed to index object ~p with error ~p because ~p",
                                           [BKey, Err, Trace])
                            end
                    end;
                false ->
                    ok
            end;
        false ->
            ok
    end.

%% @private
%%
%% @doc Update the hashtree so that AAE works but don't create any
%% indexes.  This is called when a bucket has no indexed defined.
-spec dont_index(obj(), write_reason(), p(), bkey(), short_preflist()) -> ok.
dont_index(_, delete, P, BKey, ShortPL) ->
    update_hashtree(delete, P, ShortPL, BKey),
    ok;
dont_index(Obj, _, P, BKey, ShortPL) ->
    Hash = hash_object(Obj),
    update_hashtree({insert, Hash}, P, ShortPL, BKey),
    ok.

%% @doc An object modified hook to create indexes as object data is
%% written or modified.
%%
%% NOTE: For a normal update this hook runs on the vnode process.
%%       During active anti-entropy runs on spawned process.
%%
%% NOTE: Index is doing double duty of index and delete.
-spec index(obj(), write_reason(), ring(), p(), bkey(),
            short_preflist(), index_name()) -> ok.
index(_, delete, _, P, BKey, ShortPL, Index) ->
    ok = yz_solr:delete(Index, [{bkey, BKey}]),
    ok = update_hashtree(delete, P, ShortPL, BKey),
    ok;

index(Obj, _Reason, Ring, P, BKey, ShortPL, Index) ->
    LI = yz_cover:logical_index(Ring),
    LFPN = yz_cover:logical_partition(LI, element(1, ShortPL)),
    LP = yz_cover:logical_partition(LI, P),
    Hash = hash_object(Obj),
    Docs = yz_doc:make_docs(Obj, Hash, ?INT_TO_BIN(LFPN), ?INT_TO_BIN(LP)),
    DelOp = cleanup(length(Docs), {Obj, BKey, LP}),
    ok = yz_solr:index(Index, Docs, DelOp),
    ok = update_hashtree({insert, Hash}, P, ShortPL, BKey),
    ok.

%% @doc Should the content be indexed?
-spec should_index(index_name()) -> boolean().
should_index(?YZ_INDEX_TOMBSTONE) ->
    false;
should_index(_) ->
    true.

%% @doc Perform a local KV get for `BKey' stored under `Index'.  This
%% avoids spawning a coordinator and performing quorum.
%%
%% @see yz_exchange_fsm:read_repair_keydiff/2
-spec local_get(p(), bkey()) -> {ok, obj()} | term().
local_get(Index, BKey) ->
    riak_kv_vnode:local_get(Index, BKey).

%% @doc Update AAE exchange stats for Yokozuna.
-spec update_aae_exchange_stats(p(), {p(),n()}, non_neg_integer()) -> ok.
update_aae_exchange_stats(Index, IndexN, Count) ->
    riak_kv_entropy_info:exchange_complete(yz, Index, Index, IndexN, Count),
    ok.

%% @doc Update AAE hashtree status for Yokozuna.
-spec update_aae_tree_stats(p(), non_neg_integer()) -> ok.
update_aae_tree_stats(Index, BuildTime) ->
    riak_kv_entropy_info:tree_built(yz, Index, BuildTime),
    ok.

%% @private
%%
%% @doc Update the hashtree for `Parition'/`IdxN'.
%%
%% `Action' - Either delete the `BKey' by passing `delete' or insert
%% hash by passing `{insert, ObjHash}'.
%%
%% `Partition' - The partition number of the tree to update.
%%
%% `IdxN' - The preflist the `BKey' belongs to.
%%
%% `BKey' - The bucket/key encoded as binary.
-spec update_hashtree(delete | {insert, binary()}, p(), {p(),n()},
                      {binary(), binary()}) -> ok.
update_hashtree(Action, Partition, IdxN, BKey) ->
    case get_tree(Partition) of
        not_registered ->
            ok;
        Tree ->
            Method = get_method(),
            case Action of
                {insert, ObjHash} ->
                    yz_index_hashtree:insert(Method, IdxN, BKey,
                                             ObjHash, Tree, []),
                    ok;
                delete ->
                    yz_index_hashtree:delete(Method, IdxN, BKey, Tree),
                    ok
            end
    end.

%% @private
%%
%% @doc Determine the method used to make the hashtree update.  Most
%% updates will be performed in async manner but want to occasionally
%% use a blocking call to avoid overloading the hashtree.
%%
%% NOTE: This uses the process dictionary and thus is another function
%% which relies running on a long-lived process.  In this case that
%% process is the KV vnode.  In the future this should probably use
%% cast only + sidejob for overload protection.
-spec get_method() -> async | sync.
get_method() ->
    case get(yz_hashtree_tokens) of
        undefined ->
            put(yz_hashtree_tokens, max_hashtree_tokens() - 1),
            async;
        N when N > 0 ->
            put(yz_hashtree_tokens, N - 1),
            async;
        _ ->
            put(yz_hashtree_tokens, max_hashtree_tokens() - 1),
            sync
    end.

%% @private
%%
%% @doc Return the max number of async hashtree calls that may be
%% performed before requiring a blocking call.
-spec max_hashtree_tokens() -> pos_integer().
max_hashtree_tokens() ->
    %% Use same max as riak_kv
    app_helper:get_env(riak_kv, anti_entropy_max_async, 90).

%% @doc Write a value
-spec put(any(), binary(), binary(), binary(), string()) -> ok.
put(Client, Bucket, Key, Value, ContentType) ->
    O = riak_object:new(Bucket, Key, Value, ContentType),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    N = riak_core_bucket:n_val(BucketProps),
    Client:put(O, [{pw,N},{w,N},{dw,N}]).

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
%%
%% @doc Determine whether process `Flag' is set.
-spec check_flag(term()) -> boolean().
check_flag(Flag) ->
    true == erlang:get(Flag).

%% @private
cleanup(1, {_Obj, BKey, _LP}) ->
    %% Delete any siblings
    [{siblings, BKey}];
cleanup(2, {Obj, _BKey, LP}) ->
    %% An object has crossed the threshold from
    %% being a single value Object, to a sibling
    %% value Object, delete the non-sibling ID
    DocID = yz_doc:doc_id(Obj, ?INT_TO_BIN(LP)),
    [{id, DocID}];
cleanup(_, _) ->
    [].

%% @private
%%
%% @doc Get first partition from a preflist.
first_partition([{Partition, _}|_]) ->
    Partition.

%% @private
%%
%% @doc Get the tree.
%%
%% NOTE: Relies on the fact that this code runs on the KV vnode
%%       process.
-spec get_tree(p()) -> tree() | not_registered.
get_tree(Partition) ->
    case erlang:get({tree,Partition}) of
        undefined ->
            lager:debug("Tree cache miss (undefined): ~p", [Partition]),
            get_and_set_tree(Partition);
        Pid ->
            case is_process_alive(Pid) of
                true ->
                    Pid;
                false ->
                    lager:debug("Tree cache miss (dead): ~p", [Partition]),
                    get_and_set_tree(Partition)
            end
    end.

%% @private
-spec get_and_set_tree(p()) -> tree() | not_registered.
get_and_set_tree(Partition) ->
    case yz_entropy_mgr:get_tree(Partition) of
        {ok, Tree} ->
            erlang:put({tree,Partition}, Tree),
            Tree;
        not_registered ->
            erlang:put({tree,Partition}, undefined),
            not_registered
    end.

%% @private
%%
%% @doc Determine if the local node and remote node have the same
%% metadata.
-spec is_metadata_consistent(node()) -> boolean().
is_metadata_consistent(RemoteNode) ->
    BucketTypesPrefix = {core, bucket_types},
    Server = {riak_core_metadata_hashtree, RemoteNode},
    RemoteHash = gen_server:call(Server, {prefix_hash, BucketTypesPrefix}, 1000),
    %% TODO Even though next call is local should also add 1s timeout
    %% since this call blocks vnode.  Or see above.
    LocalHash = riak_core_metadata_hashtree:prefix_hash(BucketTypesPrefix),
    LocalHash == RemoteHash.

%% @private
%%
%% @doc Determine if the `Node' is the current owner or next owner of
%%      partition `P'.
-spec is_owner_or_future_owner(p(), node(), ring()) -> boolean().
is_owner_or_future_owner(P, Node, Ring) ->
    OwnedAndNext = yz_misc:owned_and_next_partitions(Node, Ring),
    ordsets:is_element(P, OwnedAndNext).

%% @private
%%
%% @doc Determine if service is up on node.
-spec is_service_up(atom(), node()) -> boolean().
is_service_up(Service, Node) ->
    lists:member(Service, riak_core_node_watcher:services(Node)).
