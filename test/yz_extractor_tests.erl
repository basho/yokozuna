%%--------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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
%%--------------------------------------------------------------------

%% @doc EUnit and Exercise yz_extractor.
-module(yz_extractor_tests).

-compile(export_all).

-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

ensure_default_map_is_sorted_test() ->
    ok = check_default_map(?DEFAULT_MAP).

check_default_map([H | T]) ->
    {ContentType, _Extractor} = H,
    check_default_map(T, ContentType).

check_default_map([], _LastContentType) ->
    ok;
check_default_map([H | T], LastContentType) ->
    {ContentType, _Extractor} = H,
    ?assert(ContentType >= LastContentType),
    check_default_map(T, ContentType).
