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
-compile([export_all, nowarn_export_all]).
-include_lib("riak_core/include/riak_core_bucket_type.hrl").
-include("yokozuna.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
%%      hash_object/1 is left for backwards compatability of EQC tests
-spec hash_object(riak_object:riak_object()) -> binary().
hash_object(Obj) ->
    riak_object:hash(Obj).
-spec hash_object(riak_object:riak_object(), p()) -> binary().
hash_object(Obj, P) ->
    Version = riak_kv_entropy_manager:get_partition_version(P),
    riak_object:hash(Obj, Version).

%% @doc Get the content-type of the object.
-spec get_obj_ct(obj_metadata()) -> binary().
get_obj_ct(MD) ->
    case dict:find(<<"content-type">>, MD) of
        {ok, CT} -> CT;
        _ ->
            lager:warning("No Content-Type provided in the Riak Object Metadata"
                          " using ~p as a default", [?DEFAULT_CTYPE]),
            ?DEFAULT_CTYPE
    end.

-spec get_obj_bucket(obj()) -> bucket().
get_obj_bucket(Obj) ->
    riak_object:bucket(Obj).

-spec get_obj_key(obj()) -> binary().
get_obj_key(Obj) ->
    riak_object:key(Obj).

-spec get_obj_md(obj()) -> undefined | yz_dict().
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
    get_index_from_bucket_props(BProps).

get_index_from_bucket_props(BProps) ->
    proplists:get_value(?YZ_INDEX, BProps, ?YZ_INDEX_TOMBSTONE).

is_search_enabled_for_bucket(BucketProps) ->
    get_index_from_bucket_props(BucketProps) =/=
        ?YZ_INDEX_TOMBSTONE.

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
            Consistent = is_metadata_consistent(TargetNode),
            HasIndexes = has_indexes(TargetNode),
            case {Consistent, HasIndexes} of
                {true, true} ->
                    true;
                {false, false} ->
                    ?INFO("waiting for bucket types prefix and indexes to agree between ~p and ~p",
                          [node(), TargetNode]),
                    false;
                {false, _} ->
                    ?INFO("waiting for bucket types prefix to agree between ~p and ~p",
                          [node(), TargetNode]),
                    false;
                {_, false} ->
                    ?INFO("waiting for indexes to agree between ~p and ~p",
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


%% @doc Index the data supplied in the Riak Object.
%% The Riak Object should be a serialized object (a binary,
%% which has been serialized using riak_object:to_binary/1)
-spec index_binary(bucket(), key(), binary(), write_reason(), p()) -> ok.
index_binary(Bucket, Key, Bin, Reason, P) ->
    case yokozuna:is_enabled(index) andalso ?YZ_ENABLED of
        true ->
            RObj = riak_object:from_binary(Bucket, Key, Bin),
            index(
                {RObj, no_old_object}, Reason, P
            );
        _ -> ok
    end.

%% @doc Index the data supplied in the Riak Object.
-spec index(object_pair(), write_reason(), p()) -> ok.
index({Obj, _OldObj}=Objects, Reason, P) ->
    case yokozuna:is_enabled(index) andalso ?YZ_ENABLED of
        true ->
            BKey = {riak_object:bucket(Obj), riak_object:key(Obj)},
            Index = yz_kv:get_index(BKey),

            yz_solrq:index(Index, BKey, Objects, Reason, P);
        false ->
            ok
    end.

%% @doc Should the content be indexed?
-spec should_index(index_name()) -> boolean().
should_index(Index) ->
    ?YZ_SHOULD_INDEX(Index).

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
-spec update_aae_tree_stats(p(), riak_kv_entropy_info:t_now()) -> ok.
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
            case Action of
                {insert, ObjHash} ->
                    yz_index_hashtree:insert(IdxN, BKey,
                                             ObjHash, Tree, []),
                    ok;
                delete ->
                    yz_index_hashtree:delete(IdxN, BKey, Tree),
                    ok
            end
    end.

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
%%
%% @doc Get first partition from a preflist.
first_partition([{Partition, _}|_]) ->
    Partition.

%% @private
%%
%% @doc Get the tree if yz anti_entropy is enabled.
%%
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
    case yz_entropy_mgr:enabled() of
        true ->
            case yz_entropy_mgr:get_tree(Partition) of
                {ok, Tree} ->
                    erlang:put({tree,Partition}, Tree),
                    Tree;
                not_registered ->
                    erlang:put({tree,Partition}, undefined),
                    not_registered
            end;
        false ->
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

%% @private
%%
%% @doc Check if bucket props have 2.0 CRDT datatype entry and object
%%      matches-up or property for strong consistency.
-spec is_datatype_or_consistent(riak_object:riak_object(),
                                riak_kv_bucket:props()) -> boolean().
is_datatype_or_consistent(Obj, BProps) when is_list(BProps) ->
    is_datatype(Obj, BProps) orelse lists:member({consistent, true}, BProps);
is_datatype_or_consistent(_, _) -> false.

%% @private
%%
%% @doc Check if Bucket Properties contain CRDT datatype and object
%%      matches-up as a CRDT object.
%%
%%      We use the separate riak_kv_crdt functions because we want to
%%      dismiss ?TOMBSTONE (<<>>) handling from doc encoding and only
%%      to the merge after we retrun the truthiness of the datatype
%%      check.
-spec is_datatype(riak_object:riak_object(),
                  riak_kv_bucket:props()) -> boolean().
is_datatype(Obj, BProps) when is_list(BProps) ->
    case riak_kv_crdt:is_crdt_supported(BProps) of
        true ->
            ObjVals = lists:dropwhile(fun is_tombstone_value/1,
                                      riak_object:get_values(Obj)),
            riak_kv_crdt:is_crdt_object(ObjVals);
        _ ->
            false
    end;
is_datatype(_, _) -> false.

is_tombstone_value(ObjVal) ->
    ObjVal == ?TOMBSTONE.

%% @private
%%
%% @doc Check if bucket props allow for siblings.
-spec siblings_permitted(riak_object:riak_object(),
                         riak_kv_bucket:props()) -> boolean().
siblings_permitted(Obj, BProps) when is_list(BProps) ->
    case {is_datatype_or_consistent(Obj, BProps),
          proplists:get_bool(allow_mult, BProps),
          proplists:get_bool(last_write_wins, BProps)} of
        {true, _, _} -> false;
        {false, _, true} -> false;
        {false, false, _} -> false;
        {_, _, _} -> true
    end;
siblings_permitted(_, _) -> true.

%% @private
%%
%% @doc Merge siblings for objects that shouldn't have them.
-spec maybe_merge_siblings(riak_kv_bucket:props(), obj()) -> obj().
maybe_merge_siblings(BProps, Obj) ->
    case {siblings_permitted(Obj, BProps), riak_object:value_count(Obj)} of
        {true, _} ->
            Obj;
        {false, 1} ->
            Obj;
        _ ->
            case is_datatype(Obj, BProps) of
                true -> riak_kv_crdt:merge(Obj);
                false -> riak_object:reconcile([Obj], false)
            end
    end.

%%%===================================================================
%%% Tests
%%%===================================================================

-ifdef(TEST).

metadata_check_test() ->
    B = <<"hello">>,
    K = <<"foo">>,
    V = <<"heyo">>,
    O1 = riak_object:new(B, K, V),
    O2 = riak_object:new(B, K, V, "application/json"),
    %% Not dealing w/ siblings, so get the single value
    {MD1, _} = hd(riak_object:get_contents(O1)),
    {MD2, _} = hd(riak_object:get_contents(O2)),
    ?assertEqual(yz_kv:get_obj_ct(MD1), ?DEFAULT_CTYPE),
    ?assertEqual(yz_kv:get_obj_ct(MD2), "application/json").

siblings_permitted_test_() ->
{setup,
     fun() ->
             meck:new(riak_core_capability, []),
             meck:expect(riak_core_capability, get,
                         fun({riak_core, bucket_types}) -> true;
                            (X) -> meck:passthrough([X]) end),
             meck:expect(riak_core_capability, get,
                         fun({riak_kv, crdt}, []) ->
                                 [pncounter,riak_dt_pncounter,riak_dt_orswot,
                                  riak_dt_map];
                         (X, Y) -> meck:passthrough([X, Y]) end),
             application:load(riak_core),
             application:set_env(riak_core, default_bucket_props, []),
             riak_core_ring_events:start_link(),
             riak_core_ring_manager:start_link(test),
             riak_core_claimant:start_link(),
             riak_core_metadata_manager:start_link([]),
             riak_core_ring_manager:setup_ets(test),
             riak_core_metadata_hashtree:start_link(),
             ok
     end,
     fun(_) ->
             process_flag(trap_exit, true),
             riak_core_ring_manager:cleanup_ets(test),
             catch application:stop(riak_core),
             catch(exit(whereis(riak_core_riak_core_metadata_hashtreeclaimant), shutdown)),
             catch(exit(whereis(riak_core_claimant), shutdown)),
             catch(exit(whereis(riak_core_ring_manager), shutdown)),
             catch(exit(whereis(riak_core_ring_events), shutdown)),
             application:unset_env(riak_core, default_bucket_props),
             meck:unload(riak_core_capability)
     end,
     [
      ?_test(begin
                 Bucket1 = <<"bucket">>,
                 BucketType = <<"type">>,
                 Bucket2 = {BucketType, <<"bucket2">>},
                 riak_core_bucket:set_bucket(Bucket1, [{consistent, true}]),
                 riak_core_bucket_type:create(BucketType, [{consistent, true}]),
                 riak_core_bucket_type:activate(BucketType),
                 TypeProps = riak_core_bucket_type:get(BucketType),
                 ?assert(proplists:get_value(consistent, TypeProps)),
                 ?assertEqual([{consistent,true}, {name, Bucket1}],
                              riak_core_bucket:get_bucket(Bucket1)),
                 BTProps = riak_core_bucket:get_bucket(Bucket2),
                 ?assert(proplists:get_value(consistent, BTProps)),
                 ?assertEqual(Bucket2, proplists:get_value(name, BTProps)),
                 [begin
                      Object = riak_object:new(B, K, V),
                      CheckBucket = riak_object:bucket(Object),
                      CheckBucketProps = riak_core_bucket:get_bucket(CheckBucket),
                      ?assertNot(siblings_permitted(Object, CheckBucketProps))
                  end || {B, K, V} <- [{Bucket1, <<"k1">>, hi},
                                     {Bucket2, <<"k2">>, hey}]]
             end),
      ?_test(begin
                 BucketType1 = <<"counters">>,
                 BucketType2 = <<"maps">>,
                 BucketType3 = <<"loopfors">>,
                 Bucket1 = {BucketType1, <<"crdt">>},
                 Bucket2 = {BucketType2, <<"crdtz">>},
                 Bucket3 = {BucketType3, <<"crdtbz">>},
                 riak_core_bucket_type:create(BucketType1,
                                              [{datatype, counter}]),
                 riak_core_bucket_type:activate(BucketType1),
                 riak_core_bucket_type:create(BucketType2, [{datatype, map}]),
                 riak_core_bucket_type:activate(BucketType2),
                 riak_core_bucket_type:create(BucketType3, []),
                 riak_core_bucket_type:activate(BucketType3),
                 BTProps1 = riak_core_bucket:get_bucket(Bucket1),
                 BTProps2 = riak_core_bucket:get_bucket(Bucket2),
                 BTProps3 = riak_core_bucket:get_bucket(Bucket3),
                 ?assertEqual(counter, proplists:get_value(datatype, BTProps1)),
                 ?assertEqual(map, proplists:get_value(datatype, BTProps2)),
                 ?assertEqual(undefined, proplists:get_value(datatype,
                                                             BTProps3)),
                 [begin
                      Object = riak_object:new(B, K, V),
                      CheckBucket = riak_object:bucket(Object),
                      CheckBucketProps = riak_core_bucket:get_bucket(CheckBucket),
                      ?assert(siblings_permitted(Object, CheckBucketProps))
                  end || {B, K, V} <- [{Bucket1, <<"k1">>, hi},
                                     {Bucket2, <<"k2">>, hey}]],
                 [begin
                      Object = riak_kv_crdt:new(B, K, Mod),
                      CheckBucket = riak_object:bucket(Object),
                      CheckBucketProps = riak_core_bucket:get_bucket(CheckBucket),
                      ?assertNot(siblings_permitted(Object, CheckBucketProps))
                  end || {B, K, Mod} <- [{Bucket1, <<"k1">>, riak_dt_pncounter},
                                       {Bucket2, <<"k2">>, riak_dt_map}]],
                 Object2 = riak_kv_crdt:new(Bucket3, <<"kjfor3">>, riak_dt_map),
                 CheckBucket2 = riak_object:bucket(Object2),
                 CheckBucketProps2 = riak_core_bucket:get_bucket(CheckBucket2),
                 ?assert(siblings_permitted(Object2, CheckBucketProps2))
             end),
      ?_test(begin
                 Bucket1 = <<"lww">>,
                 BucketType = <<"allow_multz">>,
                 Bucket2 = {BucketType, <<"allowz">>},
                 riak_core_bucket:set_bucket(Bucket1, [{last_write_wins, true}]),
                 riak_core_bucket_type:create(BucketType, [{allow_mult, false}]),
                 riak_core_bucket_type:activate(BucketType),
                 BTProps1 = riak_core_bucket:get_bucket(Bucket1),
                 BTProps2 = riak_core_bucket:get_bucket(Bucket2),
                 ?assertEqual(true, proplists:get_bool(last_write_wins, BTProps1)),
                 ?assertEqual(false, proplists:get_bool(allow_mult, BTProps2)),
                 [begin
                      Object = riak_object:new(B, K, V),
                      CheckBucket = riak_object:bucket(Object),
                      CheckBucketProps = riak_core_bucket:get_bucket(CheckBucket),
                      ?assertNot(siblings_permitted(Object, CheckBucketProps))
                  end || {B, K, V} <- [{Bucket1, <<"k1">>, hi},
                                     {Bucket2, <<"k2">>, hey}]]
             end),
      ?_test(begin
                 Bucket1 = <<"buckety">>,
                 BucketType = <<"typey">>,
                 Bucket2 = {BucketType, <<"bucketjumpy">>},
                 riak_core_bucket:set_bucket(Bucket1, [{allow_mult, true}]),
                 riak_core_bucket_type:create(BucketType, []),
                 riak_core_bucket_type:activate(BucketType),
                 BTProps1 = riak_core_bucket:get_bucket(Bucket1),
                 BTProps2 = riak_core_bucket:get_bucket(Bucket2),
                 ?assertEqual(Bucket1, proplists:get_value(name, BTProps1)),
                 ?assertEqual(Bucket2, proplists:get_value(name, BTProps2)),
                 ?assertEqual(true, proplists:get_bool(allow_mult, BTProps2)),
                 [begin
                      Object = riak_object:new(B, K, V),
                      CheckBucket = riak_object:bucket(Object),
                      CheckBucketProps = riak_core_bucket:get_bucket(CheckBucket),
                      ?assert(siblings_permitted(Object, CheckBucketProps))
                  end || {B, K, V} <- [{Bucket1, <<"k1">>, hi},
                                     {Bucket2, <<"k2">>, hey}]]
             end)]}.

-endif.
