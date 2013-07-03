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

-module(yz_counter_extractor).

-export([extract/1, extract/2]).

-include("yokozuna.hrl").
-compile(export_all).

extract(Value) ->
    extract(Value, []).

extract(Value, Opts) ->
    FieldName = field_name(Opts),
    try
        Count = count_from_value(Value),
        [{FieldName, count_to_binary(Count)}]
    catch
        _:_ -> []
    end.

-spec field_name(proplist()) -> any().
field_name(Opts) ->
    proplists:get_value(field_name, Opts, <<"riak_kv_counter">>).

-spec count_from_value(binary()) -> integer().
count_from_value(Counter) ->
    PNCounter = riak_pn_counter:from_binary(Counter),
    riak_pn_counter:value(PNCounter).
    
-spec count_to_binary(integer()) -> binary().
count_to_binary(Count) -> 
    list_to_binary(mochinum:digits(Count)).
