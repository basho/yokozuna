%% -------------------------------------------------------------------
%%
%% yz_pb_search: PB Service for Yokozuna queries
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Implements a `riak_api_pb_service' for performing search
%% queries in Yokozuna.
-module(yz_pb_search).

-include_lib("riak_pb/include/riak_search_pb.hrl").
-include("yokozuna.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).
-compile(export_all).

-import(riak_pb_search_codec, [encode_search_doc/1]).

%% @doc init/0 callback. Returns the service internal start state.
-spec init() -> any().
init() ->
    no_state.

%% @doc decode/2 callback. Decodes an incoming message.
%%      also checks that this request has permission
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #rpbsearchqueryreq{index=Index} ->
            PermAndResource = {?YZ_SECURITY_SEARCH_PERM, {?YZ_SECURITY_INDEX, Index}},
            {ok, Msg, PermAndResource};
        _ ->
            {ok, Msg}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(Msg, State) ->
    maybe_process(yokozuna:is_enabled(search), Msg, State).

maybe_process(true, #rpbsearchqueryreq{index=Index}=Msg, State) ->
    case extract_params(Msg) of
        {ok, Params} ->
            T1 = os:timestamp(),
            try
                Result = yz_solr:dist_search(Index, Params),
                case Result of
                    {error, insufficient_vnodes_available} ->
                        yz_stat:search_fail(),
                        {error, ?YZ_ERR_NOT_ENOUGH_NODES, State};
                    {_Headers, Body} ->
                        R = mochijson2:decode(Body),
                        Resp = yz_solr:get_response(R),
                        Pairs = yz_solr:get_doc_pairs(Resp),
                        MaxScore = kvc:path([<<"maxScore">>], Resp),
                        NumFound = kvc:path([<<"numFound">>], Resp),

                        RPBResp = #rpbsearchqueryresp{
                            docs = [encode_doc(Doc) || Doc <- Pairs],
                            max_score = MaxScore,
                            num_found = NumFound
                        },
                        yz_stat:search_end(?YZ_TIME_ELAPSED(T1)),
                        {reply, RPBResp, State}
                end
            catch
                throw:not_found ->
                    yz_stat:search_fail(),
                    ErrMsg = io_lib:format(?YZ_ERR_INDEX_NOT_FOUND, [Index]),
                    {error, ErrMsg, State};
                throw:{Message, URL, Err} ->
                    yz_stat:search_fail(),
                    ?INFO("~p ~p ~p~n", [Message, URL, Err]),
                    {error, Message, State};
                _:Reason ->
                    yz_stat:search_fail(),
                    Trace = erlang:get_stacktrace(),
                    ?ERROR("~p ~p~n", [Reason, Trace]),
                    {error, ?YZ_ERR_QUERY_FAILURE, State}
            end;
        {error, missing_query} ->
            {error, "Missing query", State}
    end;
maybe_process(false, _Msg, State) ->
    {error, "Search component disabled", State}.


%% @doc process_stream/3 callback. Ignored.
process_stream(_,_,State) ->
    {ignore, State}.

%% ---------------------------------
%% Internal functions
%% ---------------------------------

extract_params(#rpbsearchqueryreq{q = <<>>}) ->
    {error, missing_query};
extract_params(#rpbsearchqueryreq{q=Query, sort=Sort,
                                  rows=Rows, start=Start,
                                  filter=Filter, fl=FieldList,
                                  df=DefaultField, op=DefaultOp}) ->
    MaybeParams = [{'q.op', DefaultOp},
                   {sort, Sort},
                   {fq, Filter},
                   {fl, default(FieldList, <<"*,score">>)},
                   {df, DefaultField},
                   {start, Start},
                   {rows, Rows}],
    Params1 = [P || P={_,V} <- MaybeParams, V /= undefined andalso V /= []],
    Params2 = [{q,Query},
               {wt,<<"json">>},
               {omitHeader,true}
               |Params1],
    {ok, Params2}.

default(undefined, Default) ->
    Default;
default([], Default) ->
    Default;
default([H|T], _) ->
    unicode:characters_to_binary(
      string:join([binary_to_list(H)]++[binary_to_list(Y)||Y <- T], ","));
default(Value, _) ->
    Value.

%% @private
%%
%% NOTE: Bypass `riak_pb_search_codec' to avoid 2-pass on `Doc'.
-spec encode_doc([{field_name(), term()}]) -> #rpbsearchdoc{}.
encode_doc(Doc) ->
    Fields = lists:foldl(fun ?MODULE:encode_field/2, [], Doc),
    #rpbsearchdoc{fields = Fields}.

%% @private
-spec encode_field({field_name(), term()}, [{field_name(), term()}]) ->
                        [{field_name(), term()}].
encode_field({Field, Val}, EncodedDoc) when is_list(Val) ->
    %% if `Val' is list then dealing with multi-valued field
    MultiVals = [riak_pb_codec:encode_pair({Field, encode_val(V)}) || V <- Val],
    MultiVals ++ EncodedDoc;
encode_field({Field, Val}, EncodedDoc) ->
    [riak_pb_codec:encode_pair({Field, encode_val(Val)})|EncodedDoc].

%% @private
%%
%% NOTE: Need to do this here because `riak_pb_codec' doesn't convert
%% numbers.
-spec encode_val(number() | atom() | binary()) -> atom() | binary().
encode_val(Val) when is_integer(Val) ->
    %% TODO Use 16B `integer_to_binary' BIF once we ditch 15B support
    ?INT_TO_BIN(Val);
encode_val(Val) when is_float(Val) ->
    %% TODO Use 16B `float_to_binary' BIF once we ditch 15B support
    ?FLOAT_TO_BIN(Val);
encode_val(Val) ->
    Val.

