%% -------------------------------------------------------------------
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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
%% @doc yz_bucket_validator verifies that a bucket type's
%%      n_val property matches any associated indexe's n_val
-module(yz_bucket_validator).
-export([validate/4]).
-include("yokozuna.hrl").

-define(FMT(S, Args), list_to_binary(lists:flatten(io_lib:format(S, Args)))).

-type prop() :: {PropName::atom(), PropValue::any()}.
-type error() :: {PropName::atom(), ErrorReason::atom()}.
-type props() :: [prop()].
-type errors() :: [error()].

%% @doc Performs two validations. The first is validating that an index
%% exists before a bucket-type/bucket can be associated to it by setting search_index.
%% The second checks that the bucket-type/bucket has the same n_val as the associated
%% index's n_val.
-spec validate(create | update,
               {riak_core_bucket_type:bucket_type(), undefined | binary()} | binary(),
               undefined | props(),
               props()) -> {props(), errors()}.
validate(_CreateOrUpdate, _Bucket, ExistingProps, BucketProps) ->
    Props = case {ExistingProps, BucketProps} of
        {undefined, BucketProps} ->
            BucketProps;
        {ExistingProps, undefined} ->
            ExistingProps;
        {ExistingProps, BucketProps} ->
            riak_core_bucket_props:merge(BucketProps, ExistingProps)
    end,
    case get_search_index_info(Props) of
        {error, no_search_index} ->
            {BucketProps, []};
        {error, Msg} ->
            {proplists:delete(search_index,BucketProps), [{search_index,Msg}]};
        {Index, INVal} ->
            BNVal = proplists:get_value(n_val, Props),
            validate_n_val(Index, INVal, BNVal, BucketProps)
    end.

%% @private
%%
-spec validate_n_val(index_name(), n(), n(), props()) -> {props(), errors()}.
validate_n_val(Index, INVal, BNVal, BucketProps) ->
    case INVal of
        INVal when INVal == BNVal ->
            {BucketProps, []};
        _ ->
            Error = ?FMT("Bucket n_val ~p must match the associated "
                         "search_index ~s n_val ~p", [BNVal,Index,INVal]),
            {proplists:delete(n_val, BucketProps), [{n_val, Error}]}
    end.

%% @private
%%
-spec get_search_index_info(props()) -> {error, atom()} | {index_name(), n()}.
get_search_index_info(Props) ->
    case proplists:get_value(search_index, Props) of
        undefined ->
            {error, no_search_index};
        ?YZ_INDEX_TOMBSTONE ->
            {error, no_search_index};
        Index ->
            case yz_index:exists(Index) of
                false ->
                    {error, ?FMT("~s does not exist", [Index])};
                true ->
                    {Index, index_n_val(Index)}
            end
    end.

%% @private
%%
%% @doc Get search_index from the bucket props, and return the
%%      index's n_val. If it doesn't exist, return undefined.
index_n_val(Index) ->
    yz_index:get_n_val(yz_index:get_index_info(Index)).
