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
%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).
%% Public API
-export([start_link/0,
    get_stats/0,
    get_stat/1,
    get_stat/2,
    index_fail/0,
    index_bad_entry/0,
    index_extract_fail/0,
    index_end/3,
    drain_end/1,
    batch_end/1,
    drain_fail/0,
    drain_timeout/0,
    drain_cancel_timeout/0,
    detected_repairs/1,
    initialize_fuse_stats/1,
    search_fail/0,
    search_end/1,
    blocked_vnode/1,
    queue_total_length/1,
    hwm_purged/1,
    fuse_blown/1,
    fuse_recovered/1,
    create_dynamic_stats/2,
    delete_dynamic_stats/2,
    perform_update/1]).

%% Testing API
-export([reset/0, stat_name/1]).

-include("yokozuna.hrl").
%% -type microseconds() :: integer().
-define(SERVER, ?MODULE).
-define(APP, ?YZ_APP_NAME).
-define(PFX, riak_stat:prefix()).

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
    riak_stat:register(?APP, stats()).

%% @doc Return current aggregation of all stats.
-spec get_stats() -> proplists:proplist() | {error, Reason :: term()}.
get_stats() ->
    riak_stat:get_app_stats(?APP).

%% @doc Return the value for the stat with the given `Name', or `undefined' if
%% not found.
-spec get_stat([atom(), ...]) -> undefined | term().
get_stat(Name) when is_list(Name) ->
    get_stat(Name, value).
get_stat(Name, SummaryName) ->
    Path = [?PFX, ?APP] ++ Name,
    case riak_stat_coordinator:get_info(Path, SummaryName) of
        {ok, [{SummaryName, Value}]} ->
            Value;
        _ ->
            undefined
    end.

%% @doc Initialize the Fuse stats subsystem for a given fuse.
-spec initialize_fuse_stats(Name :: atom()) -> ok.
initialize_fuse_stats(Name) ->
    fuse_stats_exometer:init(Name).

%% @doc Send stat updates for an index failure.
-spec index_fail() -> ok.
index_fail() ->
    update(index_fail).

%% @doc Send stat updates for an index failure.
-spec index_bad_entry() -> ok.
index_bad_entry() ->
    update(index_bad_entry).

%% @doc Send stat updates for an index failure.
-spec index_extract_fail() -> ok.
index_extract_fail() ->
    update(index_extract_fail).

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

%% @doc Send stat updates for a drain cancel timeout.
-spec drain_cancel_timeout() -> ok.
drain_cancel_timeout() ->
    update(drain_cancel_timeout).

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

%% @doc update the total queue length to the specified value
-spec queue_total_length(non_neg_integer()) -> ok.
queue_total_length(Length) ->
    update({queue_total_length, Length}).

%% @doc update the the number of purges that have occurred
-spec hwm_purged(NumPurged :: non_neg_integer()) -> ok.
hwm_purged(NumPurged) ->
    update({hwm_purged, NumPurged}).

%% @doc Update fuse recovered statistic.
-spec fuse_blown(Index :: index_name()) -> ok.
fuse_blown(Index) ->
    update({fuse_blown, Index}).

%% @doc Update fuse recovered statistic.
-spec fuse_recovered(Index :: index_name()) -> ok.
fuse_recovered(Index) ->
    update({fuse_recovered, Index}).

%% @doc Create dynamic stats for search index.
-spec create_dynamic_stats(Index :: index_name(), Stats :: [atom()]) -> [ok].
create_dynamic_stats(Index, Stats) ->
    [create({Stat, Index}) || Stat <- Stats].

%% @doc Delete dynamic stats for search index.
-spec delete_dynamic_stats(Index :: index_name(), Stats :: [atom()]) -> [ok].
delete_dynamic_stats(Index, Stats) ->
    [delete({Stat, Index}) || Stat <- Stats].

-spec reset() -> [{stat_name(), ok | {error, any()}}].
reset() ->
    [{[?PFX, ?APP | Name], catch riak_stat:reset_stat([?PFX, ?APP | Name])}
        || {Name, _Type, _Args, _Aliases} <- stats()].

%% @doc Notify specific metrics in exometer based on the `StatUpdate' term
%% passed in.
-spec perform_update(StatUpdate::term()) -> ok.
perform_update({index_end, BatchSize, Time}) ->
    update([index, latency], Time, histogram),
    update([index, throughput], BatchSize, spiral),
    update([queue, batch, throughput], 1, spiral),
    update([queue, batchsize], BatchSize, histogram);
perform_update(index_fail) ->
    update([index, fail], 1, spiral);
perform_update(index_bad_entry) ->
    update([index, bad_entry], 1, spiral);
perform_update(index_extract_fail) ->
    update([index, extract, fail], 1, spiral);
perform_update({batch_end, Time}) ->
    update([queue, batch, latency], Time, histogram);
perform_update(blockedvnode) ->
    update([blockedvnode], 1, spiral);
perform_update({queue_total_length, Length}) ->
    update([queue, total_length], Length, gauge);
perform_update({hwm_purged, NumPurged}) ->
    update([queue, hwm, purged], NumPurged, spiral);
perform_update({drain_end, Time}) ->
    update([queue, drain, latency], Time, histogram),
    update([queue, drain], 1, spiral);
perform_update(drain_fail) ->
    update([queue, drain, fail], 1, spiral);
perform_update(drain_timeout) ->
    update([queue, drain, timeout], 1, spiral);
perform_update(drain_cancel_timeout) ->
    update([queue, drain, cancel, timeout], 1, spiral);
perform_update({detected_repairs, Count}) ->
    update([detected_repairs], Count, counter);
perform_update({search_end, Time}) ->
    update(['query', latency], Time, histogram),
    update(['query', throughput], 1, spiral);
perform_update(search_fail) ->
    update(['query', fail], 1, spiral);
perform_update({fuse_recovered, Index}) ->
    update([yz_fuse, yz_fuse:fuse_name_for_index(Index), recovered], 1, spiral);
perform_update({fuse_blown, Index}) ->
    update([yz_fuse, yz_fuse:fuse_name_for_index(Index), blown], 1, spiral);
perform_update({update_fuse_stat, Name, Counter}) ->
    fuse_stats_exometer:increment(Name, Counter).

update(Name, Val, Type) ->
    riak_stat:update(stat_name(Name), Val, Type).


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
    FuseName = yz_fuse:fuse_name_for_index(Index),
    riak_stat:update([yz_fuse, FuseName, recovered], 0, spiral);
create({fuse_blown, Index}) ->
    FuseName = yz_fuse:fuse_name_for_index(Index),
    riak_stat:update([yz_fuse, FuseName, blown], 0, spiral);
create(_Stat) ->
    ok.

%% @private
%%
%% @doc Delete specific dynamic metrics in exometer based on the `StatUpdate'
%%      term.
-spec delete(StatUpdate::term()) -> ok.
delete({fuse_recovered, Index}) ->
    riak_stat:unregister([fuse, Index, recovered]);
delete(_Stat) ->
    ok.

%% @private
%%
%% @doc Determine the correct channel through which to send the
%% `StatUpdate'.
-spec update(term()) -> ok.
update(StatUpdate) ->
    case sidejob:resource_exists(yz_stat_sj) of
        true -> yz_stat_worker:update(StatUpdate);
        false -> perform_update(StatUpdate)
    end,
    ok.

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
    {[index, bad_entry], spiral, [], [
        {count, search_index_bad_entry_count},
        {one,   search_index_bad_entry_one}
    ]},
    {[index, extract, fail], spiral, [], [
        {count, search_index_extract_fail_count},
        {one,   search_index_extract_fail_one}
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
    {[queue, total_length], gauge, [], [
        {value, search_queue_total_length}
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
    {[queue, drain, cancel, timeout], spiral, [], [
        {count, search_queue_drain_cancel_timeout_count},
        {one,   search_queue_drain_cancel_timeout_one}
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
