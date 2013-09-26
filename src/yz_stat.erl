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
-define(SERVER, ?MODULE).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Register Yokozuna stats.
-spec register_stats() -> ok.
register_stats() ->
    delete_metrics(),
    [register_stat(Stat, Type) || {Stat, Type} <- stats()],
    riak_core_stat_cache:register_app(?YZ_APP_NAME, {?MODULE, produce_stats, []}),
    ok.

%% @doc Return current aggregation of all stats.
-spec get_stats() -> proplists:proplist() | {error, term()}.
get_stats() ->
    case riak_core_stat_cache:get_stats(?YZ_APP_NAME) of
        {ok, Stats, _TS} -> Stats;
        Error -> Error
    end.

produce_stats() ->
    {?YZ_APP_NAME, [{Name, get_metric_value({?YZ_APP_NAME, Name}, Type)} || {Name, Type} <- stats()]}.

index_begin() ->
    do_update(index_begin).

index_fail() ->
    do_update(index_fail).

index_end(ElapsedTime) ->
    do_update({index_end, ElapsedTime}).

search_begin() ->
    do_update(search_begin).

search_fail() ->
    do_update(search_fail).

search_end(ElapsedTime) ->
    do_update({search_end, ElapsedTime}).

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
%% @doc Delete any metrics currently registered under the Yokozuna
%% application.
-spec delete_metrics() -> ok.
delete_metrics() ->
    [(catch folsom_metrics:delete_metric(Stat))
     || {?YZ_APP_NAME, Stat} <- folsom_metrics:get_metrics()],
    ok.

%% @private
%%
%% @doc Update specific metrics in folsom based on the `Stat' term
%% passed in.
-spec do_update(Stat::term()) -> ok.
do_update(index_begin) ->
    folsom_metrics:notify_existing_metric({?YZ_APP_NAME, index_pending}, {inc, 1}, counter);
do_update({index_end, Time}) ->
    folsom_metrics:notify_existing_metric({?YZ_APP_NAME, index_latency}, Time, histogram),
    folsom_metrics:notify_existing_metric({?YZ_APP_NAME, index_throughput}, 1, spiral),
    folsom_metrics:notify_existing_metric({?YZ_APP_NAME, index_pending}, {dec, 1}, counter);
do_update(index_fail) ->
    folsom_metrics:notify_existing_metric({?YZ_APP_NAME, index_pending}, {dec, 1}, counter);
do_update(search_begin) ->
    folsom_metrics:notify_existing_metric({?YZ_APP_NAME, search_pending}, {inc, 1}, counter);
do_update({search_end, Time}) ->
    folsom_metrics:notify_existing_metric({?YZ_APP_NAME, search_latency}, Time, histogram),
    folsom_metrics:notify_existing_metric({?YZ_APP_NAME, search_throughput}, 1, spiral),
    folsom_metrics:notify_existing_metric({?YZ_APP_NAME, search_pending}, {dec, 1}, counter);
do_update(search_fail) ->
    folsom_metrics:notify_existing_metric({?YZ_APP_NAME, search_pending}, {dec, 1}, counter).

%% @private
get_metric_value(Name, histogram) ->
    folsom_metrics:get_histogram_statistics(Name);
get_metric_value(Name, _Type) ->
    folsom_metrics:get_metric_value(Name).

%% @private
get_sample_type(Name) ->
    SampleType0 = app_helper:get_env(riak_kv, stat_sample_type, {slide_uniform, {60, 1028}}),
    app_helper:get_env(?YZ_APP_NAME, Name, SampleType0).

%% @private
register_stat(Name, histogram) ->
    {SampleType, SampleArgs} = get_sample_type(Name),
    folsom_metrics:new_histogram({?YZ_APP_NAME, Name}, SampleType, SampleArgs);
register_stat(Name, spiral) ->
    folsom_metrics:new_spiral({?YZ_APP_NAME, Name});
register_stat(Name, counter) ->
    folsom_metrics:new_counter({?YZ_APP_NAME, Name}).

%% @private
stats() ->
    [
     {index_latency, histogram},
     {index_pending, counter},
     {index_throughput, spiral},
     {search_pending, counter},
     {search_latency, histogram},
     {search_throughput, spiral}
    ].
