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
-module(yz_kv_hooks).
-include("yokozuna.hrl").

-define(HOOK, {struct,
    [{<<"mod">>, atom_to_binary(?MODULE, utf8)},
        {<<"fun">>, <<"precommit">>}]}).

%% API
-export([precommit/1, conditional_hook/3, install_hooks/0]).

install_hooks() ->
    riak_kv_hooks:add_conditional_precommit({?MODULE, conditional_hook}).

conditional_hook(_BucketTYpe, _Bucket, BucketProps) ->
    SearchEnabled = yokozuna:is_enabled(index) andalso ?YZ_ENABLED,
    maybe_return_hook(SearchEnabled, BucketProps).

maybe_return_hook(true, BucketProps) ->
    case yz_kv:is_search_enabled_for_bucket(BucketProps) of
        true ->
           ?HOOK;
        _ ->
            false
    end;

maybe_return_hook(false, _BucketProps) ->
    false.

precommit(Obj) ->
    %% Since this is a conditional precommit, it won't run unless we know search
    %% is enabled for the bucket in question.
    Bucket = riak_object:bucket(Obj),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    Index = yz_kv:get_index_from_bucket_props(BucketProps),
    IndexLength = yz_solrq_throttle_manager:get_index_length(Index),
    riak_core_throttle:throttle_by_load(yokozuna, ?YZ_PUT_THROTTLE_KEY, IndexLength),
    Obj.

