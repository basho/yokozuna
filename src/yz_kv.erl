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

-spec client() -> any().
client() ->
    {ok,C} = riak:local_client(),
    C.

-spec get(any(), binary(), binary()) -> any().
get(C, Bucket, Key) ->
    case C:get(Bucket, Key) of
        {ok, O} ->
            {value, riak_object:get_value(O)};
        Other ->
            Other
    end.

%% @doc Get the content-type of the object.
-spec get_obj_ct(obj()) -> binary().
get_obj_ct(MD) ->
    dict:fetch(<<"content-type">>, MD).

%% @doc Determine if the `Obj' is a tombstone.
-spec is_tombstone(obj()) -> boolean().
is_tombstone(MD) ->
    case yz_misc:dict_get(<<"X-Riak-Deleted">>, MD, false) of
        "true" -> true;
        false -> false
    end.

get_md_entry(MD, Key) ->
    yz_misc:dict_get(Key, MD, none).

delete_siblings_by_key(Obj, Bucket, Key, AllowMult) ->
    XML = yz_solr:encode_delete({key, Key}),
    yz_solr:delete_by_query(binary_to_list(Bucket), XML).

%% @doc An object modified hook to create indexes as object data is
%% written or modified.
%%
%% NOTE: This code runs on the vnode process.
%%
%% NOTE: Index is doing double duty of index and delete.
-spec index(obj(), write_reason(), term()) -> ok.
index(Obj, delete, VNodeState) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    LI = yz_cover:logical_index(Ring),
    {Bucket, Key} = BKey = {riak_object:bucket(Obj), riak_object:key(Obj)},
    BProps = riak_core_bucket:get_bucket(Bucket, Ring),
    AllowMult = proplists:get_value(allow_mult, BProps, false),
    Partition = get_partition(VNodeState),
    LP = yz_cover:logical_partition(LI, Partition),
    DocID = binary_to_list(yz_doc:doc_id(Obj, ?INT_TO_BIN(LP))),
    try
        yz_solr:delete(binary_to_list(Bucket), DocID),
        delete_siblings_by_key(Obj, Bucket, Key, AllowMult)
    catch _:Err ->
        ?ERROR("failed to delete docid ~p with error ~p", [BKey, Err])
    end;

index(Obj, Reason, VNodeState) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    LI = yz_cover:logical_index(Ring),
    {Bucket, Key} = BKey = {riak_object:bucket(Obj), riak_object:key(Obj)},
    ok = maybe_wait(Reason, Bucket),
    BProps = riak_core_bucket:get_bucket(Bucket, Ring),
    AllowMult = proplists:get_value(allow_mult, BProps),
    NVal = riak_core_bucket:n_val(BProps),
    Idx = riak_core_util:chash_key(BKey),
    IdealPreflist = lists:sublist(riak_core_ring:preflist(Idx, Ring), NVal),
    LFPN = yz_cover:logical_partition(LI, first_partition(IdealPreflist)),
    LP = yz_cover:logical_partition(LI, get_partition(VNodeState)),
    Docs = yz_doc:make_docs(Obj, ?INT_TO_BIN(LFPN), ?INT_TO_BIN(LP)),

    try
        ok = yz_solr:index(binary_to_list(Bucket), Docs),
        case riak_object:value_count(Obj) of
            2 ->
                case AllowMult of
                    true  ->
                        % An object has crossed the threshold from being a single value
                        % Object, to a sibling value Object, delete the non-sibling ID
                        DocID = binary_to_list(yz_doc:doc_id(Obj, ?INT_TO_BIN(LP))),
                        yz_solr:delete(binary_to_list(Bucket), DocID);
                    _ -> ok
                end;
            % Delete any siblings
            1 -> delete_siblings_by_key(Obj, Bucket, Key, AllowMult);
            _ -> ok
        end
    catch _:Err ->
        ?ERROR("failed to index object ~p with error ~p", [BKey, Err])
    end.

%% @doc Install the object modified hook on the given `Bucket'.
-spec install_hook(binary()) -> ok.
install_hook(Bucket) when is_binary(Bucket) ->
    Mod = yz_kv,
    Fun = index,
    ok = riak_kv_vnode:add_obj_modified_hook(Bucket, Mod, Fun).

%% @doc Uninstall the object modified hook on the given `Bucket'.
-spec uninstall_hook(binary()) -> ok.
uninstall_hook(Bucket) when is_binary(Bucket) ->
    Mod = yz_kv,
    Fun = index,
    ok = remove_obj_modified_hook(Bucket, Mod, Fun).

%% @doc Write a value
-spec put(any(), binary(), binary(), binary(), string()) -> ok.
put(Client, Bucket, Key, Value, ContentType) ->
    O = riak_object:new(Bucket, Key, Value, ContentType),
    Client:put(O, [{pw,3},{w,3},{dw,3}]).

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
%% @doc Wait for index creation if hook was invoked for handoff write.
-spec maybe_wait(write_reason(), binary()) -> ok.
maybe_wait(handoff, Bucket) ->
    Index = binary_to_list(Bucket),
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
