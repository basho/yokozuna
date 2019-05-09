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

%% @doc A noop extractor. It performs no work, creates no index fields
%%      for the registered mime type by returning an empty.
%%
-module(yz_noop_extractor).
-export([extract/1, extract/2]).

-include("yokozuna.hrl").
-define(NOOP_RESULTS, []).

extract(Value) ->
    extract(Value, ?NO_OPTIONS).

-spec extract(binary(), proplist()) -> [{binary(), binary()}] |
                                       {error, any()}.
extract(_Value, _Opts) ->
    ?NOOP_RESULTS.
