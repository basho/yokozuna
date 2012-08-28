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

-module(yz_doc).
-compile(export_all).
-include("yokozuna.hrl").

%% @doc Functionality for working with Yokozuna documents.

%%%===================================================================
%%% API
%%%===================================================================

add_to_doc({doc, Fields}, Field) ->
    {doc, [Field|Fields]}.

%% @doc Given an object generate the doc to be indexed by Solr.
-spec make_doc(riak_object:riak_object(), binary(), binary()) -> doc().
make_doc(O, FPN, Partition) ->
    %% TODO: For now assume text/plain to prototype
    %%
    %% TODO: change 'text' to 'value'
    Fields = [{id, doc_id(O, Partition)},
              {text, value(O)},
              {?YZ_ENTROPY_DATA_FIELD, gen_vc(O)},
              {'_fpn', FPN},
              {'_node', ?ATOM_TO_BIN(node())},
              {'_pn', Partition},
              {'_rk', riak_key(O)}],
    {doc, Fields}.

%%%===================================================================
%%% Private
%%%===================================================================

-spec doc_id(riak_object:riak_object(), binary()) -> binary().
doc_id(O, Partition) ->
    <<(riak_object:key(O))/binary,"_",Partition/binary>>.

%% TODO: Just pass metadata in?
%%
%% TODO: I don't like having X-Riak-Last-Modified in here.  Add
%%       function to riak_object.
doc_ts(O) ->
    MD = riak_object:get_metadata(O),
    dict:fetch(<<"X-Riak-Last-Modified">>, MD).

doc_vclock(O) ->
    riak_object:vclock(O).

gen_ts() ->
    {{Year, Month, Day},
     {Hour, Min, Sec}} = calendar:now_to_universal_time(erlang:now()),
    list_to_binary(io_lib:format("~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0B",
                                 [Year,Month,Day,Hour,Min,Sec])).

gen_vc(O) ->
    TS = gen_ts(),
    RiakKey = riak_key(O),
    VClock = base64:encode(crypto:sha(term_to_binary(doc_vclock(O)))),
    <<TS/binary," ",RiakKey/binary," ",VClock/binary>>.

riak_key(O) ->
    riak_object:key(O).

value(O) ->
    riak_object:get_value(O).
