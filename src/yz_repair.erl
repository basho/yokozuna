%% -------------------------------------------------------------------
%%
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

-module(yz_repair).

%% @doc a module to check yz consistency
%% use this via attached console like this:
%% > yz_repair:check_type(<<"some_buckettype">>)
%% this finds a yokozuna index tied to bucket-types property
%% or
%% > yz_repair:check_type(<<"some_bucket">>)
%% this finds a yokozuna index tied to bucket property

%% note that this tool is not so much efficient, because
%% this fetches all keys from memory ... in near future
%% this script should be a vnode iterator that joins with
%% keys from yokozuna popped out

-export([check/1, repair/1,
         check_type/1, repair_type/1,
         check_bucket/1, repair_bucket/1,
         check_vnode/4]).

%% somewhat different from original bucket types,
%% just for comprehensiveness in code below.
-type bucket_or_type() :: bucket() | type().
-type bucket() :: {bucket, binary()}.
-type type() :: {type, binary()}.

-spec check_type(binary()) -> ok.
check_type(BucketType) ->
    check({type, BucketType}).

-spec check_bucket(binary()) -> ok.
check_bucket(Bucket) ->
    check({bucket, Bucket}).

-spec check(bucket_or_type()) -> ok.
check(BucketOrType) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {RingSize, CHash} = riak_core_ring:chash(Ring),
    Checker = folder(RingSize, BucketOrType, false),
    lists:foldl(Checker, [], CHash).

-spec repair_type(binary()) -> ok.
repair_type(BucketType) ->
    repair({type, BucketType}).
-spec repair_bucket(binary()) -> ok.
repair_bucket(Bucket) ->
    repair({bucket, Bucket}).

-spec repair(bucket_or_type()) -> ok.
repair(BucketOrType) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {RingSize, CHash} = riak_core_ring:chash(Ring),
    Repairer = folder(RingSize, BucketOrType, true),
    lists:foldl(Repairer, [], CHash).

-spec folder(non_neg_integer(), bucket_or_type(), boolean()) ->
                     [{non_neg_integer(), term(), term()}].
folder(RingSize, BucketOrType, DoRepair) ->
    %% VNode = n * 2^160 / RingSize
    %% n = VNode * RingSize / 2^160 = 1 bsl 160
    fun(VNode, Acc0) ->
            {VNodeID, _} = VNode,
            Id = VNodeID * RingSize div (1 bsl 160) + 1,
            case check_vnode(VNode, Id, BucketOrType, DoRepair) of
                ok -> Acc0;
                {error, Reason} -> [{Id, VNode, Reason}|Acc0]
            end
    end.

-spec check_vnode(term(), non_neg_integer(), bucket_or_type(), boolean()) -> ok.
check_vnode(VNode, Id, Bucket, DoRepair) ->
    {Found, KeysFromSolr} = keys_from_solr(VNode, Id, Bucket),
    KeysFromRiak = list_keys(VNode, Bucket),

    case length(KeysFromSolr) == length(KeysFromRiak) of
        true ->
            %% actually it's not okay by just counting
            io:format("partition ~p is ok.~n", [Id]);
        false ->
            io:format("partition ~p> keys from riak: ~p vs ~p (~p): keys from solr ~n",
                      [Id, length(KeysFromRiak), length(KeysFromSolr), Found]),

            case KeysFromRiak -- KeysFromSolr of
                [] -> ok;
                Diff0 ->
                    %% We can fix it!!!
                    io:format("~p keys are only in Riak.~n", [ length(Diff0) ]),
                    repair_keys(Diff0, DoRepair)
            end,
            
            case KeysFromSolr -- KeysFromRiak of
                [] -> ok;
                Diff1 ->
                    %% We can't fix it.
                    io:format("~p are keys only in Solr", [ length(Diff1) ])
            end
    end.

list_keys(VNode, {type, BucketType}) ->
    Lister = fun({{Type,_},_} = BK, _V, Acc) when Type =:= BucketType ->
                     [BK|Acc];
                (_, _, Acc) -> Acc
             end,
    riak_kv_vnode:fold(VNode,Lister,[]);

list_keys(VNode, {bucket, Bucket}) ->
    ReqID = 3245,
    ok = riak_kv_vnode:list_keys(VNode, ReqID, self(), Bucket),
    wait_for_list_keys(ReqID, []).
%% [{VN, riak_kv_vnode:fold(VN, fun(_,_,Acc) -> Acc+1 end, 0)}
%%  || VN <- element(2, element(4, Ring))].

wait_for_list_keys(ReqID, Acc) ->
    receive
        {ReqID, {kl, _N, List}} ->
            wait_for_list_keys(ReqID, [List|Acc]);
        {ReqID, _Index, done} ->
            lists:flatten(Acc);
        A ->
            io:format("error: ~p~n", [A]),
            Acc
    end.

keys_from_solr({_, Remote} = _VNode, Id, BucketOrType) ->
    IndexName = binary_to_existing_atom(get_index_name(BucketOrType),
                                        latin1),
    Q = build_query(Id, BucketOrType),
    Params = [{q, lists:flatten(Q)},{rows,1000000},{wt, json}],
    {_Headers, Resp} = rpc:call(Remote, yz_solr, search,
                                [IndexName, [], Params]),
    Obj = mochijson2:decode(Resp),
    Found = kvc:path([<<"response">>, <<"numFound">>], Obj),
    Docs0 = kvc:path([<<"response">>, <<"docs">>], Obj),
    Docs = docs2tbk(BucketOrType, Docs0),
    
    %% io:format("~p keys found from solr at pn=~p~n", [Found, Id]),
    %% io:format("is_list:~p~n", [is_list(Docs)]),
    %%io:format("~p", [KeysFromSolr]),
    {Found, Docs}.

-spec docs2tbk(bucket_or_type(), [{struct, list()}]) ->
                      [{{binary(),binary()} | binary(),binary()}].
docs2tbk({type, BucketType}, Docs) ->
    lists:map(fun({struct, Proplists}) ->
                      Bucket = proplists:get_value(<<"_yz_rb">>, Proplists),
                      Key = proplists:get_value(<<"_yz_rk">>, Proplists),
                      {{BucketType, Bucket}, Key}
              end, Docs);
docs2tbk({bucket, Bucket}, Docs) ->
    lists:map(fun({struct, Proplists}) ->
                      Key = proplists:get_value(<<"_yz_rk">>, Proplists),
                      {Bucket, Key}
              end, Docs).

build_query(Id, {type, BucketType}) ->
    io_lib:format("_yz_pn:~p AND _yz_rt:~s", [Id, BucketType]);
build_query(Id, {bucket, Bucket}) ->
    io_lib:format("_yz_pn:~p AND _yz_rb:~s", [Id, Bucket]).

get_index_name(BucketOrType) ->
    {ok, C} = riak:local_client(),
    Props =
        case BucketOrType of
            {type, BucketType} ->
                riak_core_bucket_type:get(BucketType);
            {bucket, Bucket} ->
                riak_client:get_bucket(Bucket, C)
        end,
    IndexName = proplists:get_value(search_index, Props),
    true = (IndexName =/= undefined),
    IndexName.

repair_keys(_Diff, false) ->  ok;
repair_keys([], _) -> ok;
repair_keys(Diff, true) ->
    {ok, C} = riak:local_client(),
    {OK, Err} = repair_keys(C, Diff, {0, []}),
    io:format("~p keys repaired while ~p keys were not repaired~n",
              [OK, length(Err)]).

repair_keys(_, [], Result) -> Result;
repair_keys(C, [{Bucket,Key}|Diff], {OK, Err}) ->
    {ok, RiakObject} = riak_client:get(Bucket, Key, [], C),
    case riak_client:put(RiakObject, C) of
        ok -> %% fixed
            repair_keys(C, Diff, {OK+1, Err});
        E ->  %% not fixed
            repair_keys(C, Diff, {OK, [E|Err]})
    end.
