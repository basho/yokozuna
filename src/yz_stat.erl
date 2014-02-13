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
-define(NOTIFY(A, B, Type, Arg),
        folsom_metrics:notify_existing_metric({?YZ_APP_NAME, A, B}, Arg, Type)).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Register Yokozuna stats.
-spec register_stats() -> ok.
register_stats() ->
    [begin
         Name = stat_name(Path),
         (catch folsom_metrics:delete_metric(Name)),
         register_stat(Name, Type)
     end || {Path, Type} <- stats()],
    riak_core_stat_cache:register_app(?YZ_APP_NAME, {?MODULE, produce_stats, []}),
    ok.

%% @doc Return current aggregation of all stats.
-spec get_stats() -> proplists:proplist() | {error, term()}.
get_stats() ->
    case riak_core_stat_cache:get_stats(?YZ_APP_NAME) of
        {ok, Stats, _TS} -> Stats;
        Error -> Error
    end.

%% @doc Return formatted stats
-spec get_formatted_stats() -> [term()].
get_formatted_stats() ->
    case yokozuna:is_enabled(index) andalso ?YZ_ENABLED of
        false -> 
            [];
        true -> 
            Stats = ?MODULE:get_stats(),
            format_stats(Stats)
    end.

%% TODO: export stats() type from riak_core_stat_q.
-spec produce_stats() -> {atom(), list()}.
produce_stats() ->
    {?YZ_APP_NAME, riak_core_stat_q:get_stats([yokozuna])}.

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
-spec notify(StatUpdate::term()) -> ok.
notify({index_end, Time}) ->
    ?NOTIFY(index, latency, histogram, Time),
    ?NOTIFY(index, throughput, spiral, 1);
notify(index_fail) ->
    ?NOTIFY(index, fail, spiral, 1);
notify({search_end, Time}) ->
    ?NOTIFY(search, latency, histogram, Time),
    ?NOTIFY(search, throughput, spiral, 1);
notify(search_fail) ->
    ?NOTIFY(search, fail, spiral, 1).

%% @private
get_sample_type(Name) ->
    SampleType0 = app_helper:get_env(riak_kv, stat_sample_type, {slide_uniform, {60, 1028}}),
    app_helper:get_env(?YZ_APP_NAME, Name, SampleType0).

%% @private
-spec register_stat(riak_core_stat_q:stat_name(), atom()) -> ok |
                                                             {error, Subject :: term(), Reason :: term()}.
register_stat(Name, histogram) ->
    {SampleType, SampleArgs} = get_sample_type(Name),
    folsom_metrics:new_histogram(Name, SampleType, SampleArgs);
register_stat(Name, spiral) ->
    folsom_metrics:new_spiral(Name);
register_stat(Name, counter) ->
    folsom_metrics:new_counter(Name).

%% @private
-spec stats() -> [{riak_core_stat_q:path(), atom()}].
stats() ->
    [
     {[index, fail], spiral},
     {[index, latency], histogram},
     {[index, throughput], spiral},
     {[search, fail], spiral},
     {[search, latency], histogram},
     {[search, throughput], spiral}
    ].

%% @private
-spec stat_name(riak_core_stat_q:path()) -> riak_core_stat_q:stat_name().
stat_name(Name) ->
    list_to_tuple([?YZ_APP_NAME|Name]).

%% @private
%%
%% @doc Determine the correct channel through which to send the
%% `StatUpdate'.
-spec update(term()) -> ok.
update(StatUpdate) ->
    case erlang:module_loaded(yz_stat_sj) of
        true -> yz_stat_worker:update(StatUpdate);
        false -> notify(StatUpdate)
    end,
    ok.

%% @private
%%
%% @doc Format stats for legacy stats blob
-spec format_stats([term()]) -> [term()].
format_stats(Stats) ->
    lists:flatten([format(Stat) || Stat <- Stats]).

format({{_, search, fail},[{count, Count}, {one, One}]}) ->
    [{search_query_fail_count, Count},
     {search_query_fail_one, One}];
format({{_, search, throughput},[{count, Count}, {one, One}]}) ->
    [{search_query_throughput_count, Count},
     {search_query_throughput_one, One}];
format({{_, index, fail},[{count, Count}, {one, One}]}) ->
    [{search_index_fail_count, Count},
     {search_index_fail_one, One}];
format({{_, index, throughput},[{count, Count}, {one, One}]}) ->
    [{search_index_throughput_count, Count},
     {search_index_throughput_one, One}];
format({{_, search, latency}, ValueList}) ->
    [format_query_latency(V) || V <- ValueList];
format({{_, index, latency}, ValueList}) ->
    [format_index_latency(V) || V <- ValueList];
format({yokozuna_stat_ts, TS}) ->
    [{search_stat_ts, TS}];
format(T) -> T.

format_query_latency({min, Value}) ->
    {search_query_latency_min, Value};
format_query_latency({max, Value}) ->
    {search_query_latency_max, Value};
format_query_latency({median, Value}) ->
    {search_query_latency_median, Value};
format_query_latency({percentile, PList}) ->
    [ format_query_latency_percentile(P) || P <- PList];
format_query_latency({_, _}) ->
    [].

format_query_latency_percentile({95, Value}) ->
    {search_query_latency_percentile_95, Value};
format_query_latency_percentile({99, Value}) ->
    {search_query_latency_percentile_99, Value};
format_query_latency_percentile(_) -> [].

format_index_latency({min, Value}) ->
    {search_index_latency_min, Value};
format_index_latency({max, Value}) ->
    {search_index_latency_max, Value};
format_index_latency({median, Value}) ->
    {search_index_latency_median, Value};
format_index_latency({percentile, PList}) ->
    [ format_index_latency_percentile(P) || P <- PList];
format_index_latency({_, _}) ->
    [].

format_index_latency_percentile({95, Value}) ->
    {search_index_latency_percentile_95, Value};
format_index_latency_percentile({99, Value}) ->
    {search_index_latency_percentile_99, Value};
format_index_latency_percentile(_) -> [].

