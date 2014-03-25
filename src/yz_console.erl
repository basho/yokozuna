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
-module(yz_console).
-include("yokozuna.hrl").
-export([aae_status/1]).

%% @doc Print the Active Anti-Entropy status to stdout.
-spec aae_status([]) -> ok.
aae_status([]) ->
    ExchangeInfo = yz_kv:compute_exchange_info(),
    riak_kv_console:aae_exchange_status(ExchangeInfo),
    io:format("~n"),
    TreeInfo = yz_kv:compute_tree_info(),
    riak_kv_console:aae_tree_status(TreeInfo),
    io:format("~n"),
    riak_kv_console:aae_repair_status(ExchangeInfo),
    ok.
