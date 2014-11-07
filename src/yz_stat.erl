%% -------------------------------------------------------------------
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
%%
%% @doc yz_stat is a module for aggregating Yokozuna statistics,
%%      from administration to querying
-module(yz_stat).
-behaviour(gen_server).
-compile(export_all).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("yokozuna.hrl").
%% -type microseconds() :: integer().
-define(SERVER, ?MODULE).
-define(APP, ?YZ_APP_NAME).
-define(PFX, riak_core_stat:prefix()).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Register Yokozuna stats.
-spec register_stats() -> ok.
register_stats() ->
    riak_core_stat:register_stats(?APP, stats()).

%% @doc Return current aggregation of all stats.
-spec get_stats() -> proplists:proplist() | {error, term()}.
get_stats() ->
    riak_core_stat:get_stats(?APP).

%% @doc Send stat updates for an index failure.
-spec index_fail() -> ok.
index_fail() ->
    update(index_fail).

%% @doc Send stat updates for an index completion.  `ElapsedTime'
%% should be microseconds.
-spec index_end(integer()) -> ok.
index_end(ElapsedTime) ->
    update({index_end, ElapsedTime}).

%% @doc Send stat updates for a search failure.
-spec search_fail() -> ok.
search_fail() ->
    update(search_fail).

%% @doc Send stat updates for search completion.  `ElapsedTime' should
%% be microseconds.
-spec search_end(integer()) -> ok.
search_end(ElapsedTime) ->
    update({search_end, ElapsedTime}).


%% @doc Optionally produce stats map based on ?YZ_ENABLED
-spec stats_map() -> [] | proplists:proplist().
stats_map() ->
     stats_map(?YZ_ENABLED).

%% @doc Map to format stats for legacy "blob" if YZ_ENABLED,
%% else [].
-spec stats_map(true | false) -> [] | proplists:proplist().
stats_map(false) -> [];
stats_map(true) ->
     [
      %% Query stats
      {search_query_throughput_count, {{?YZ_APP_NAME, 'query', throughput}, count}, spiral},
      {search_query_throughput_one, {{?YZ_APP_NAME, 'query', throughput}, one}, spiral},
      {search_query_fail_count, {{?YZ_APP_NAME, 'query', fail}, count}, spiral},
      {search_query_fail_one, {{?YZ_APP_NAME, 'query', fail}, one}, spiral},
      {search_query_latency_min, {{?YZ_APP_NAME, 'query', latency}, min}, histogram},
      {search_query_latency_max, {{?YZ_APP_NAME, 'query', latency}, max}, histogram},
      {search_query_latency_median, {{?YZ_APP_NAME, 'query', latency}, median}, histogram},
      {search_query_latency_95, {{?YZ_APP_NAME, 'query', latency}, 95}, histogram_percentile},
      {search_query_latency_99, {{?YZ_APP_NAME, 'query', latency}, 99}, histogram_percentile},
      {search_query_latency_999, {{?YZ_APP_NAME, 'query', latency}, 999}, histogram_percentile},

      %% Index stats
      {search_index_throughput_count, {{?YZ_APP_NAME, index, throughput}, count}, spiral},
      {search_index_throughtput_one, {{?YZ_APP_NAME, index, throughput}, one}, spiral},
      {search_index_fail_count, {{?YZ_APP_NAME, index, fail}, count}, spiral},
      {search_index_fail_one, {{?YZ_APP_NAME, index, fail}, one}, spiral},
      {search_index_latency_min, {{?YZ_APP_NAME, index, latency}, min}, histogram},
      {search_index_latency_max, {{?YZ_APP_NAME, index, latency}, max}, histogram},
      {search_index_latency_median, {{?YZ_APP_NAME, index, latency}, median}, histogram},
      {search_index_latency_95, {{?YZ_APP_NAME, index, latency}, 95}, histogram_percentile},
      {search_index_latency_99, {{?YZ_APP_NAME, index, latency}, 99}, histogram_percentile},
      {search_index_latency_999, {{?YZ_APP_NAME, index, latency}, 999}, histogram_percentile}].

%% -------------------------------------------------------------------
%% Callbacks
%% -------------------------------------------------------------------

init([]) ->
    register_stats(),
    {ok, ok}.

handle_call(_Req, _From, State) ->
    ?WARN("Unexpected request received ~p", [_Req]),
    {reply, ok, State}.

handle_cast(_Req, State) ->
    ?WARN("Unexpected request received ~p", [_Req]),
    {noreply, State}.

handle_info(_Info, State) ->
    ?WARN("Unexpected request received ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -------------------------------------------------------------------
%% Private
%% -------------------------------------------------------------------

%% @private
%%
%% @doc Notify specific metrics in folsom based on the `StatUpdate' term
%% passed in.
-spec update(StatUpdate::term()) -> ok.
update({index_end, Time}) ->
    exometer:update([?PFX, ?APP, index, latency], Time),
    exometer:update([?PFX, ?APP, index, throughput], 1);
update(index_fail) ->
    exometer:update([?PFX, ?APP, index, fail], 1);
update({search_end, Time}) ->
    exometer:update([?PFX, ?APP, 'query', latency], Time),
    exometer:update([?PFX, ?APP, 'query', throughput], 1);
update(search_fail) ->
    exometer:update([?PFX, ?APP, 'query', fail], 1).

%% @private
-spec stats() -> [{riak_core_stat_q:path(), atom()}].
stats() ->
    [
     {[index, fail], spiral},
     {[index, latency], histogram},
     {[index, throughput], spiral},
     {['query', fail], spiral},
     {['query', latency], histogram},
     {['query', throughput], spiral}
    ].
