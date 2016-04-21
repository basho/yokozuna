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

-type stat_name() :: list().
-type stat_type() :: atom().
-type stat_opts() :: [tuple()].
-type stat_map()  :: [{atom() | integer(), atom()}].

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
-spec get_stats() -> proplists:proplist() | {error, Reason :: term()}.
get_stats() ->
    riak_core_stat:get_stats(?APP).

%% @doc Send stat updates for an index failure.
-spec index_fail() -> ok.
index_fail() ->
    update(index_fail).

%% @doc Send stat updates for an index completion.  `ElapsedTime'
%% should be microseconds.
-spec index_end(index_name(),
                BatchSize :: integer(),
                ElapsedTime :: integer()) -> ok.
index_end(_Index, BatchSize, ElapsedTime) ->
    update({index_end, BatchSize, ElapsedTime}).


%% @doc Send stat updates for a drain completion.  `ElapsedTime'
%% should be microseconds.
-spec drain_end(ElapsedTime :: integer()) -> ok.
drain_end(ElapsedTime) ->
    update({drain_end, ElapsedTime}).


%% @doc Send stat updates for a batch completion.  `ElapsedTime'
%% should be microseconds.
-spec batch_end(ElapsedTime :: integer()) -> ok.
batch_end(ElapsedTime) ->
    update({batch_end, ElapsedTime}).

%% @doc Send stat updates for a drain failure.
-spec drain_fail() -> ok.
drain_fail() ->
    update(drain_fail).

%% @doc Send stat updates for a drain timeout.
-spec drain_timeout() -> ok.
drain_timeout() ->
    update(drain_timeout).

%% @doc Send updates for aae repairs.
-spec detected_repairs(Count :: integer()) -> ok.
detected_repairs(Count) ->
    update({detected_repairs, Count}).

%% @doc Send stat updates for a search failure.
-spec search_fail() -> ok.
search_fail() ->
    update(search_fail).

%% @doc Send stat updates for search completion.  `ElapsedTime' should
%% be microseconds.
-spec search_end(ElapsedTime :: integer()) -> ok.
search_end(ElapsedTime) ->
    update({search_end, ElapsedTime}).

%% @doc Count of times the solrq had to block a vnode
%%      pass the vnode From into the function for redbugging,
%%      cannot see any value in overhead of doing stats
%%      by worker pool
blocked_vnode(_From) ->
    update(blockedvnode).

%% @doc update the queue capacity to the specified value, as a percentage in [0..100]
-spec queue_capacity(capacity()) -> ok.
queue_capacity(Capacity) ->
    update({queue_capacity, Capacity}).

%% @doc update the the number of purges that have occurred
-spec hwm_purged(NumPurged :: non_neg_integer()) -> ok.
hwm_purged(NumPurged) ->
    update({hwm_purged, NumPurged}).

%% @doc Update fuse recovered statistic.
-spec fuse_recovered(Index :: atom()) -> ok.
fuse_recovered(Index) ->
    update({fuse_recovered, Index}).

%% @doc Create dynamic stats for search index.
-spec create_dynamic_stats(Index :: atom(), Stats :: [atom()]) -> [ok].
create_dynamic_stats(Index, Stats) ->
    [create({Stat, Index}) || Stat <- Stats].

%% @doc Delete dynamic stats for search index.
-spec delete_dynamic_stats(Index :: atom(), Stats :: [atom()]) -> [ok].
delete_dynamic_stats(Index, Stats) ->
    [delete({Stat, Index}) || Stat <- Stats].

-spec reset() -> [{stat_name(), ok | {error, any()}}].
reset() ->
    [{[?PFX, ?APP | Name], catch exometer:reset([?PFX, ?APP | Name])}
        || {Name, _Type, _Args, _Aliases} <- stats()].

%% @doc Optionally produce stats map based on ?YZ_ENABLED
-spec stats_map() -> [] | proplists:proplist().
stats_map() ->
     stats_map(?YZ_ENABLED).

%% @doc Map to format stats for legacy "blob" if YZ_ENABLED,
%% else [].
-spec stats_map(true | false) -> [] | proplists:proplist().
stats_map(false) -> [];
stats_map(true) -> [
    %% Index stats
    {search_index_throughput_count, {{?YZ_APP_NAME, index, throughput}, count}, spiral},
    {search_index_throughput_one, {{?YZ_APP_NAME, index, throughput}, one}, spiral},

    {search_index_fail_count, {{?YZ_APP_NAME, index, fail}, count}, spiral},
    {search_index_fail_one, {{?YZ_APP_NAME, index, fail}, one}, spiral},

    {search_index_latency_min, {{?YZ_APP_NAME, index, latency}, min}, histogram},
    {search_index_latency_max, {{?YZ_APP_NAME, index, latency}, max}, histogram},
    {search_index_latency_mean, {{?YZ_APP_NAME, index, latency}, mean}, histogram},
    {search_index_latency_median, {{?YZ_APP_NAME, index, latency}, median}, histogram},
    {search_index_latency_95, {{?YZ_APP_NAME, index, latency}, 95}, histogram_percentile},
    {search_index_latency_99, {{?YZ_APP_NAME, index, latency}, 99}, histogram_percentile},
    {search_index_latency_999, {{?YZ_APP_NAME, index, latency}, 999}, histogram_percentile},

    {search_queue_batch_throughput_count, {{?YZ_APP_NAME, queue, batch, throughput}, count}, spiral},
    {search_queue_batch_throughput_one, {{?YZ_APP_NAME, queue, batch, throughput}, one}, spiral},

    {search_queue_batchsize_min, {{?YZ_APP_NAME, queue, batchsize}, min}, histogram},
    {search_queue_batchsize_max, {{?YZ_APP_NAME, queue, batchsize}, max}, histogram},
    {search_index_latency_mean, {{?YZ_APP_NAME, queue, batchsize}, mean}, histogram},
    {search_index_latency_median, {{?YZ_APP_NAME, queue, batchsize}, median}, histogram},

    {search_queue_capacity, {{?YZ_APP_NAME, queue, capacity}, value}, gauge},

    {search_queue_hwm_purged_count, {{?YZ_APP_NAME, queue, hwm, purged}, count}, spiral},
    {search_queue_hwm_purged_one, {{?YZ_APP_NAME, queue, hwm, purged}, one}, spiral},

    {search_blockedvnode_count, {{?YZ_APP_NAME, blockedvnode}, count}, spiral},
    {search_blockedvnode_one, {{?YZ_APP_NAME, blockedvnode}, one}, spiral},

    {search_queue_drain_count, {{?YZ_APP_NAME, queue, drain}, count}, spiral},
    {search_queue_drain_one, {{?YZ_APP_NAME, queue, drain}, one}, spiral},

    {search_queue_drain_fail_count, {{?YZ_APP_NAME, queue, drain, fail}, count}, spiral},
    {search_queue_drain_fail_one, {{?YZ_APP_NAME, queue, drain, fail}, one}, spiral},

    {search_queue_drain_timeout_count, {{?YZ_APP_NAME, queue, drain, timeout}, count}, spiral},
    {search_queue_drain_timeout_one, {{?YZ_APP_NAME, queue, drain, timeout}, one}, spiral},

    {search_queue_drain_latency_min, {{?YZ_APP_NAME, queue, drain, latency}, min}, histogram},
    {search_queue_drain_latency_max, {{?YZ_APP_NAME, queue, drain, latency}, max}, histogram},
    {search_queue_drain_latency_mean, {{?YZ_APP_NAME, queue, drain, latency}, mean}, histogram},
    {search_queue_drain_latency_median, {{?YZ_APP_NAME, queue, drain, latency}, median}, histogram},
    {search_queue_drain_latency_95, {{?YZ_APP_NAME, queue, drain, latency}, 95}, histogram_percentile},
    {search_queue_drain_latency_99, {{?YZ_APP_NAME, queue, drain, latency}, 99}, histogram_percentile},
    {search_queue_drain_latency_999, {{?YZ_APP_NAME, queue, drain, latency}, 999}, histogram_percentile},

    {search_queue_batch_latency_min, {{?YZ_APP_NAME, queue, batch, latency}, min}, histogram},
    {search_queue_batch_latency_max, {{?YZ_APP_NAME, queue, batch, latency}, max}, histogram},
    {search_queue_batch_latency_mean, {{?YZ_APP_NAME, queue, batch, latency}, mean}, histogram},
    {search_queue_batch_latency_median, {{?YZ_APP_NAME, queue, batch, latency}, median}, histogram},
    {search_queue_batch_latency_95, {{?YZ_APP_NAME, queue, batch, latency}, 95}, histogram_percentile},
    {search_queue_batch_latency_99, {{?YZ_APP_NAME, queue, batch, latency}, 99}, histogram_percentile},
    {search_queue_batch_latency_999, {{?YZ_APP_NAME, queue, batch, latency}, 999}, histogram_percentile},

    {search_detected_repairs_count, {{?YZ_APP_NAME, detected_repairs}, value}, counter},

    %% Query stats
    {search_query_throughput_count, {{?YZ_APP_NAME, 'query', throughput}, count}, spiral},
    {search_query_throughput_one, {{?YZ_APP_NAME, 'query', throughput}, one}, spiral},
    {search_query_fail_count, {{?YZ_APP_NAME, 'query', fail}, count}, spiral},
    {search_query_fail_one, {{?YZ_APP_NAME, 'query', fail}, one}, spiral},
    {search_query_latency_min, {{?YZ_APP_NAME, 'query', latency}, min}, histogram},
    {search_query_latency_mean, {{?YZ_APP_NAME, 'query', latency}, mean}, histogram},
    {search_query_latency_max, {{?YZ_APP_NAME, 'query', latency}, max}, histogram},
    {search_query_latency_median, {{?YZ_APP_NAME, 'query', latency}, median}, histogram},
    {search_query_latency_95, {{?YZ_APP_NAME, 'query', latency}, 95}, histogram_percentile},
    {search_query_latency_99, {{?YZ_APP_NAME, 'query', latency}, 99}, histogram_percentile},
    {search_query_latency_999, {{?YZ_APP_NAME, 'query', latency}, 999}, histogram_percentile}
].

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

-spec stat_name(riak_core_stat_q:path()) -> riak_core_stat_q:stat_name().
stat_name(Name) ->
    [?PFX, ?APP | Name].

%% @private
%%
%% @doc Create specific dynamic metrics in exometer based on the `StatUpdate'
%%      term.
-spec create(StatUpdate::term()) -> ok.
create({fuse_recovered, Index}) ->
    exometer:update_or_create([fuse, Index, recovered], 0, spiral, []);
create(_Stat) ->
    ok.

%% @private
%%
%% @doc Delete specific dynamic metrics in exometer based on the `StatUpdate'
%%      term.
-spec delete(StatUpdate::term()) -> ok.
delete({fuse_recovered, Index}) ->
    exometer:delete([fuse, Index, recovered]);
delete(_Stat) ->
    ok.

%% @private
%%
%% @doc Notify specific metrics in exometer based on the `StatUpdate' term
%% passed in.
-spec update(StatUpdate::term()) -> ok.
update({index_end, BatchSize, Time}) ->
    exometer:update([?PFX, ?APP, index, latency], Time),
    exometer:update([?PFX, ?APP, index, throughput], BatchSize),
    exometer:update([?PFX, ?APP, queue, batch, throughput], 1),
    exometer:update([?PFX, ?APP, queue, batchsize], BatchSize);
update(index_fail) ->
    exometer:update([?PFX, ?APP, index, fail], 1);
update({batch_end, Time}) ->
    exometer:update([?PFX, ?APP, queue, batch, latency], Time);
update(blockedvnode) ->
    exometer:update([?PFX, ?APP, blockedvnode], 1);
update({queue_capacity, Length}) ->
    exometer:update([?PFX, ?APP, queue, capacity], Length);
update({hwm_purged, NumPurged}) ->
    exometer:update([?PFX, ?APP, queue, hwm, purged], NumPurged);
update({drain_end, Time}) ->
    exometer:update([?PFX, ?APP, queue, drain, latency], Time),
    exometer:update([?PFX, ?APP, queue, drain], 1);
update(drain_fail) ->
    exometer:update([?PFX, ?APP, queue, drain, fail], 1);
update(drain_timeout) ->
    exometer:update([?PFX, ?APP, queue, drain, timeout], 1);
update({detected_repairs, Count}) ->
    exometer:update([?PFX, ?APP, detected_repairs], Count);
update({search_end, Time}) ->
    exometer:update([?PFX, ?APP, 'query', latency], Time),
    exometer:update([?PFX, ?APP, 'query', throughput], 1);
update(search_fail) ->
    exometer:update([?PFX, ?APP, 'query', fail], 1);
update({fuse_recovered, Index}) ->
    exometer:update([fuse, Index, recovered], 1).

%% @private
-spec stats() -> [{stat_name(), stat_type(), stat_opts(), stat_map()}].
stats() -> [
    {[index, throughput], spiral, [], [
        {count, search_index_throughput_count},
        {one,   search_index_throughput_one}
    ]},
    {[index, fail], spiral, [], [
        {count, search_index_fail_count},
        {one,   search_index_fail_one}
    ]},
    {[index, latency], histogram, [], [
        {min,    search_index_latency_min},
        {max,    search_index_latency_max},
        {mean,   search_index_latency_mean},
        {median, search_index_latency_median},
        {95,     search_index_latency_95},
        {99,     search_index_latency_99},
        {999,    search_index_latency_999}
    ]},
    {[queue, batch, throughput], spiral, [], [
        {count, search_queue_batch_throughput_count},
        {one,   search_queue_batch_throughput_one}
    ]},
    {[queue, batchsize], histogram, [], [
        {min,    search_queue_batchsize_min},
        {max,    search_queue_batchsize_max},
        {mean,   search_queue_batchsize_mean},
        {median, search_queue_batchsize_median}
    ]},
    {[queue, hwm, purged], spiral, [], [
        {count, search_queue_hwm_purged_count},
        {one,   search_queue_hwm_purged_one}
    ]},
    {[queue, capacity], gauge, [], [
        {value, search_queue_capacity}
    ]},
    {[blockedvnode], spiral, [], [
        {count, search_blockedvnode_count},
        {one,   search_blockedvnode_one}
    ]},
    {[queue, drain], spiral, [], [
         {count, search_queue_drain_count},
         {one,   search_queue_drain_one}
    ]},
    {[queue, drain, fail], spiral, [], [
        {count, search_queue_drain_fail_count},
        {one,   search_queue_drain_fail_one}
    ]},
    {[queue, drain, timeout], spiral, [], [
        {count, search_queue_drain_timeout_count},
        {one,   search_queue_drain_timeout_one}
    ]},
    {[queue, drain, latency], histogram, [], [
        {min,    search_queue_drain_latency_min},
        {max,    search_queue_drain_latency_max},
        {mean,   search_queue_drain_latency_mean},
        {median, search_queue_drain_latency_median},
        {95,     search_queue_drain_latency_95},
        {99,     search_queue_drain_latency_99},
        {999,    search_queue_drain_latency_999}
    ]},
    {[queue, batch, latency], histogram, [], [
        {min,    search_queue_batch_latency_min},
        {max,    search_queue_batch_latency_max},
        {mean,   search_queue_batch_latency_mean},
        {median, search_queue_batch_latency_median},
        {95,     search_queue_batch_latency_95},
        {99,     search_queue_batch_latency_99},
        {999,    search_queue_batch_latency_999}
    ]},
    {[detected_repairs], counter, [], [
        {value,    search_detected_repairs_count}
    ]},
    {['query', fail], spiral, [], [
        {count, search_query_fail_count},
        {one  , search_query_fail_one}
    ]},
    {['query', latency], histogram, [], [
        {95    , search_query_latency_95},
        {99    , search_query_latency_99},
        {999   , search_query_latency_999},
        {max   , search_query_latency_max},
        {median, search_query_latency_median},
        {min   , search_query_latency_min},
        {mean  , search_query_latency_mean}
    ]},
    {['query', throughput], spiral, [], [
        {count,search_query_throughput_count},
        {one  ,search_query_throughput_one}
    ]}
] ++ yz_fuse:stats().
