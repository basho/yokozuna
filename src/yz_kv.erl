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

%%%===================================================================
%%% API
%%%===================================================================

index_obj(Obj, VNodeState) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {Bucket, _} = BKey = {riak_object:bucket(Obj), riak_object:key(Obj)},
    BProps = riak_core_bucket:get_bucket(Bucket, Ring),
    NVal = riak_core_bucket:n_val(BProps),
    Idx = riak_core_util:chash_key(BKey),
    IdealPreflist = riak_core_ring:preflist(Idx, NVal, Ring),
    FPN = ?INT_TO_BIN(first_partition(IdealPreflist)),
    Doc = yz_doc:make_doc(Obj),
    Doc2 = yz_doc:add_to_doc(Doc, {'_fpn', FPN}),
    Doc3 = add_partition(Doc2, ?INT_TO_BIN(get_partition(VNodeState))),
    yz_solr:index(binary_to_list(Bucket), [Doc3]).

install_obj_modified_hook(Bucket) when is_binary(Bucket) ->
    Mod = yz_kv,
    Fun = index_obj,
    ok = riak_kv_vnode:add_obj_modified_hook(Bucket, Mod, Fun).


%%%===================================================================
%%% Private
%%%===================================================================

add_partition(Doc, Partition) ->
    yz_doc:add_to_doc(Doc, {'_pn', Partition}).

first_partition([{Partition, _}|_]) ->
    Partition.

get_partition(VNodeState) ->
    riak_kv_vnode:get_state_partition(VNodeState).

handle_index_cmd(Index, Doc, Partition) ->
    Doc2 = add_partition(Doc, Partition),
    yz_solr:index(Index, [Doc2]).
