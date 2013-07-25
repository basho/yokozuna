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
decode(Code, Bin) ->
    {ok, riak_pb_codec:decode(Code, Bin)}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(#rpbsearchqueryreq{index=IndexBin}=Msg, State) ->
    case extract_params(Msg) of
        {ok, Params} ->
            Mapping = yz_events:get_mapping(),
            Index = binary_to_list(IndexBin),
            try
                Result = yz_solr:dist_search(Index, Params, Mapping),
                case Result of
                    {error, insufficient_vnodes_available} ->
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
                        {reply, RPBResp, State}
                end
            catch
                throw:not_found ->
                    ErrMsg = io_lib:format(?YZ_ERR_INDEX_NOT_FOUND, [Index]),
                    {error, ErrMsg, State};
                throw:{Message, URL, Err} ->
                    ?INFO("~p ~p ~p~n", [Message, URL, Err]),
                    {error, Message, State};
                _:Reason ->
                    Trace = erlang:get_stacktrace(),
                    ?ERROR("~p ~p~n", [Reason, Trace]),
                    {error, ?YZ_ERR_QUERY_FAILURE, State}
            end;
        {error, missing_query} ->
            {error, "Missing query", State}
    end.

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
    {ok, [{q, Query},
          {wt, "json"},
          {omitHeader, true},
          {'q.op', default(DefaultOp, "AND")},
          {sort, default(Sort, "")},
          {fq, default(Filter, "")},
          {fl, default(FieldList, <<"*,score">>)},
          {df, default(DefaultField, "")},
          {start, default(Start, 0)},
          {rows, default(Rows, 10)}]}.

default(undefined, Default) ->
    Default;
default([], Default) ->
    Default;
default([H|T], _) ->
    unicode:characters_to_binary(
      string:join([binary_to_list(H)]++[binary_to_list(Y)||Y <- T], ","));
default(Value, _) ->
    Value.

encode_doc(Doc) ->
    EncodedDoc = lists:foldl(fun ?MODULE:encode_field/2, [], Doc),
    riak_pb_search_codec:encode_search_doc(EncodedDoc).

encode_field({Prop, Val}, EncodedDoc) when is_list(Val) ->
    %% if `Val' is list then dealing with multi-valued field
    MultiVals = [{Prop, to_binary(V)} || V <- Val],
    MultiVals ++ EncodedDoc;
encode_field({Prop, V}, EncodedDoc) ->
    [{Prop, to_binary(V)}|EncodedDoc].

to_binary(B) when is_binary(B) -> B;
to_binary(I) when is_integer(I) -> list_to_binary(integer_to_list(I));
to_binary(F) when is_float(F) -> list_to_binary(float_to_list(F)).
