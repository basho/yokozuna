%% -------------------------------------------------------------------
%% Copyright (c) 2015 Basho Technologies, Inc. All Rights Reserved.
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
-module(yz_fuse).
-include("yokozuna.hrl").

%% setup
-export([setup/0]).

%% api
-export([create/1, check/1, melt/1, reset/1]).

%% helpers
-export([fuse_context/0]).

%% stats helpers
-export([aggregate_index_stats/2, stats/0, get_stats_for_index/1]).

-define(APP, ?YZ_APP_NAME).

%%%===================================================================
%%% Setup
%%%===================================================================

%% @doc Start fuse and stats
-spec setup() -> ok.
setup() ->
    ok = yokozuna:ensure_started(fuse),

    %% Set up fuse stats
    application:set_env(fuse, stats_plugin, fuse_stats_exometer).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(index_name()|atom()) -> ok | reset | {error, _}.
create(Index) ->
    IndexName = ?BIN_TO_ATOM(Index),
    case check(IndexName) of
        {error, not_found} ->
            ?INFO("Creating fuse for search index ~s", [Index]),
            MaxR = app_helper:get_env(yokozuna, melt_attempts, 3),
            MaxT = app_helper:get_env(yokozuna, melt_time_window, 5000),
            Refresh = {reset, app_helper:get_env(yokozuna, melt_reset_refresh, 30000)},
            Strategy = {standard, MaxR, MaxT},
            Opts = {Strategy, Refresh},
            fuse:install(IndexName, Opts);
        _ -> ok
end.

-spec reset(index_name()) -> ok | {error, not_found}.
reset(Index) ->
    fuse:reset(?BIN_TO_ATOM(Index)).

-spec check(index_name()|atom()) -> ok | blown | {error, not_found}.
check(Index) when is_binary(Index) ->
    check(?BIN_TO_ATOM(Index));
check(Index) ->
    fuse:ask(Index, fuse_context()).

-spec melt(index_name()) -> ok.
melt(Index) ->
    fuse:melt(?BIN_TO_ATOM(Index)).

%%%===================================================================
%%% Helpers
%%%===================================================================

-spec fuse_context() -> async_dirty | sync.
fuse_context() ->
    app_helper:get_env(?YZ_APP_NAME, fuse_context, async_dirty).

%%%===================================================================
%%% Stats
%%%===================================================================

stats() ->
    Spec = fun(N, M, F, As) ->
               {[N], {function, M, F, As, match, value}, [], [{value, N}]}
           end,
    [Spec(N, M, F, As) ||
        {N, M, F, As} <- [{search_index_fuses_ok_count, yz_fuse,
                          aggregate_index_stats, [ok, count]},
                         {search_index_fuses_ok_one, yz_fuse,
                          aggregate_index_stats, [ok, one]},
                         {search_index_fuses_melt_count, yz_fuse,
                          aggregate_index_stats, [melt, count]},
                         {search_index_fuses_melt_one, yz_fuse,
                          aggregate_index_stats, [melt, one]},
                         {search_index_fuses_blown_count, yz_fuse,
                          aggregate_index_stats, [blown, count]},
                         {search_index_fuses_blown_one, yz_fuse,
                          aggregate_index_stats, [blown, one]}]].

aggregate_index_stats(FuseCheck, Stat) ->
    proplists:get_value(Stat,
                        exometer:aggregate([{{[fuse, '_', FuseCheck],'_','_'},
                                             [], [true]}], [Stat])).

-spec get_stats_for_index(atom()) -> ok.
get_stats_for_index(Index) ->
    case check(Index) of
        {error, _} ->
            io:format("No stats found for index ~s\n", [Index]);
        _ ->
            lists:foreach(
              fun(Check) ->
                      {ok, Stats} = exometer:get_value([fuse, Index, Check]),
                      io:format("Index - ~s: count: ~p | one: | ~p for fuse stat `~s`\n",
                                [Index, proplists:get_value(count, Stats),
                                 proplists:get_value(one, Stats), Check])
              end, [ok, melt, blown])
    end.
