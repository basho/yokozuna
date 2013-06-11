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
-include("yokozuna.hrl").

-define(ONE_SECOND, 1000).
-define(WAIT_FLAG(Index), {wait_flag, Index}).
-define(MAX_WAIT_FOR_INDEX, app_helper:get_env(?YZ_APP_NAME, max_wait_for_index_seconds, 5)).

-type check() :: {module(), atom(), list()}.
-type seconds() :: pos_integer().
-type write_reason() :: delete | handoff | put.


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

-spec hash_object(obj()) -> binary().
hash_object(Obj) ->
    Vclock = riak_object:vclock(Obj),
    Obj2 = riak_object:set_vclock(Obj, lists:sort(Vclock)),
    Hash = erlang:phash2(term_to_binary(Obj2)),
    term_to_binary(Hash).

%% @doc Get the content-type of the object.
-spec get_obj_ct(obj_metadata()) -> binary().
get_obj_ct(MD) ->
    dict:fetch(<<"content-type">>, MD).

-spec get_obj_bucket(obj()) -> binary().
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
-spec get_tree_build_time(tree()) -> timestamp().
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

index(Obj, Reason, VNodeState) ->
    case yokozuna:noop_flag(index) orelse not ?YZ_ENABLED of
        true ->
            ok;
        false ->
            Ring = yz_misc:get_ring(transformed),
            P = get_partition(VNodeState),
            case is_owner_or_future_owner(P, node(), Ring) of
                true ->
                    {Bucket, _} = BKey = {riak_object:bucket(Obj), riak_object:key(Obj)},
                    BProps = riak_core_bucket:get_bucket(Bucket, Ring),
                    NVal = riak_core_bucket:n_val(BProps),
                    PrimaryPL = yz_misc:primary_preflist(BKey, Ring, NVal),
                    Index = which_index(BProps),
                    IndexContent = index_content(Index),
                    IdxN = {first_partition(PrimaryPL), NVal},
                    index(Obj, Reason, Ring, P, BKey, IdxN, Index, IndexContent);
                false ->
                    ok
            end
    end.

%% @doc An object modified hook to create indexes as object data is
%% written or modified.
%%
%% NOTE: For a normal update this hook runs on the vnode process.
%%       During active anti-entropy runs on spawned process.
%%
%% NOTE: Index is doing double duty of index and delete.
-spec index(obj(), write_reason(), ring(), p(), bkey(),
            {p(), n()}, index_name(), boolean()) -> ok.
index(_, delete, _, P, BKey, IdxN, Index, _) ->
    {_, Key} = BKey,
    try
        Query = yz_solr:encode_delete({key, Key}),
        ok = yz_solr:delete_by_query(Index, Query),
        ok = update_hashtree(delete, P, IdxN, BKey)
    catch _:Err ->
            ?ERROR("failed to delete docid ~p with error ~p", [BKey, Err])
    end,
    ok;

index(Obj, Reason, Ring, P, BKey, IdxN, Index, IndexContent) ->
    LI = yz_cover:logical_index(Ring),
    {_, Key} = BKey,
    ok = maybe_wait(Reason, Index),
    LFPN = yz_cover:logical_partition(LI, element(1, IdxN)),
    LP = yz_cover:logical_partition(LI, P),
    Docs = yz_doc:make_docs(Obj, ?INT_TO_BIN(LFPN), ?INT_TO_BIN(LP),
                            IndexContent),
    try
        ok = yz_solr:index(Index, Docs),
        ok = cleanup(length(Docs), {Index, Obj, Key, LP}),
        ok = update_hashtree({insert, yz_kv:hash_object(Obj)},
                             P, IdxN, BKey)
    catch _:Err ->
        Trace = erlang:get_stacktrace(),
        ?ERROR("failed to index object ~p with error ~p because ~p~n", [BKey, Err, Trace])
    end,
    ok.

%% @doc Should the content be indexed?
-spec index_content(index_name()) -> boolean().
index_content(?YZ_DEFAULT_INDEX) ->
    false;
index_content(_) ->
    true.

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

-spec update_hashtree(delete | {insert, binary()}, p(), {p(),n()},
                      {binary(), binary()}) -> ok.
update_hashtree(Action, Partition, IdxN, BKey) ->
    try
        case get_tree(Partition) of
            not_registered ->
                ok;
            Tree ->
                case Action of
                    {insert, ObjHash} ->
                        yz_index_hashtree:insert(sync, IdxN, BKey,
                                                 ObjHash, Tree, []);
                    delete ->
                        yz_index_hashtree:delete(sync, IdxN, BKey, Tree)
                end
        end
    catch _:Reason ->
            lager:debug("Failed to update hashtree: ~p ~p", [BKey, Reason])
    end.

%% @doc Write a value
-spec put(any(), binary(), binary(), binary(), string()) -> ok.
put(Client, Bucket, Key, Value, ContentType) ->
    O = riak_object:new(Bucket, Key, Value, ContentType),
    Ring = yz_misc:get_ring(transformed),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    N = proplists:get_value(n_val, BucketProps),
    Client:put(O, [{pw,N},{w,N},{dw,N}]).

%% @doc Remove the `Index' property from `Bucket'.  Data stored under
%%      `Bucket' will no longer be indexed.
-spec remove_index(bucket()) -> ok.
remove_index(Bucket) ->
    set_index(Bucket, ?YZ_DEFAULT_INDEX).

%% @doc Set the `Index' for which data stored in `Bucket' should be
%%      indexed under.
-spec set_index(bucket(), index_name()) -> ok.
set_index(Bucket, Index) ->
    ok = riak_core_bucket:set_bucket(Bucket, [{?YZ_INDEX, Index}]).


%% @doc Extract the index name from `BProps' or return the default
%%      index name if there is none.
%%
%% TODO: Convert `index_name()' to be binary everywhere.
-spec which_index(term()) -> index_name().
which_index(BProps) ->
    case proplists:get_value(?YZ_INDEX, BProps, ?YZ_DEFAULT_INDEX) of
        B when is_binary(B) ->
            binary_to_list(B);
        S ->
            S
    end.

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
%%
%% @docs remove a hook from the bucket
%%
%% NOTE: Move this into riak_kv
remove_obj_modified_hook(Bucket, Mod, Fun) ->
    BProps = riak_core_bucket:get_bucket(Bucket),
    Existing = proplists:get_value(obj_modified_hooks, BProps, []),
    HookProp = {Mod, Fun},
    Hooks = lists:delete(HookProp, Existing),
    ok = riak_core_bucket:set_bucket(Bucket, [{obj_modified_hooks, Hooks}]).


%% @private
%%
%% @doc Determine whether process `Flag' is set.
-spec check_flag(term()) -> boolean().
check_flag(Flag) ->
    true == erlang:get(Flag).

%% @private
cleanup(1, {Index, _Obj, Key, _LP}) ->
    %% Delete any siblings
    JSON = yz_solr:encode_delete({key, Key, siblings}),
    ok = yz_solr:delete_by_query(Index, JSON);

cleanup(2, {Index, Obj, _Key, LP}) ->
    %% An object has crossed the threshold from
    %% being a single value Object, to a sibling
    %% value Object, delete the non-sibling ID
    DocID = yz_doc:doc_id(Obj, ?INT_TO_BIN(LP)),
    ok = yz_solr:delete(Index, DocID);

cleanup(_, _) ->
    ok.

%% @private
%%
%% @doc Get first partition from a preflist.
first_partition([{Partition, _}|_]) ->
    Partition.

%% @private
%%
%% @doc Get the partition from the `VNodeState'.
get_partition(VNodeState) ->
    riak_kv_vnode:get_state_partition(VNodeState).

%% @private
%%
%% @doc Get the tree.
%%
%% NOTE: Relies on the fact that this code runs on the KV vnode
%%       process.
-spec get_tree(p()) -> tree() | not_registered.
get_tree(Partition) ->
    case erlang:get(tree) of
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
            erlang:put(tree, Tree),
            Tree;
        not_registered ->
            erlang:put(tree, undefined),
            not_registered
    end.

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
%% @doc Wait for index creation if hook was invoked for handoff write.
%%
%% NOTE: This function assumes it is running on a long-lived process.
-spec maybe_wait(write_reason(), index_name()) -> ok.
maybe_wait(handoff, Index) ->
    Flag = ?WAIT_FLAG(Index),
    case check_flag(Flag) of
        false ->
            Seconds = ?MAX_WAIT_FOR_INDEX,
            ok = wait_for({yz_solr, ping, [Index]}, Seconds),
            ok = set_flag(Flag);
        true ->
            ok
    end;
maybe_wait(_, _) ->
    ok.

%% @doc Set the `Flag'.
-spec set_flag(term()) -> ok.
set_flag(Flag) ->
    erlang:put(Flag, true),
    ok.

%% @doc Wait for `Check' for the given number of `Seconds'.
-spec wait_for(check(), seconds()) -> ok.
wait_for(_, 0) ->
    ok;
wait_for(Check={M,F,A}, Seconds) when Seconds > 0 ->
    case M:F(A) of
        true ->
            ok;
        false ->
            timer:sleep(?ONE_SECOND),
            wait_for(Check, Seconds - 1)
    end.
