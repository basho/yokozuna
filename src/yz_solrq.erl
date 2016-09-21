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
-module(yz_solrq).

-export([index/5,
         worker_regname/2,
         helper_regname/2,
         set_hwm/1,
         set_index/4,
         set_purge_strategy/1,
         reload_appenv/0,
         blown_fuse/1,
         healed_fuse/1,
         solrq_worker_names/0,
         queue_total_length/0,
         get_max_batch_size/0,
         get_min_batch_size/0,
         get_flush_interval/0,
         all_solrq_workers/0,
         solrq_workers_for_partition/1,
         solrq_worker_pairs_for_index/1]).

-include("yokozuna.hrl").

% for debugging only
-export([status/0]).

-type regname() :: atom().
-type size_resps() :: same_size | {shrank, non_neg_integer()} |
                      {grew, non_neg_integer()}.

-define(SOLRQS_TUPLE_KEY, solrqs_tuple).
-define(SOLRQ_HELPERS_TUPLE_KEY, solrq_helpers_tuple).

-export_type([regname/0, size_resps/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec index(index_name(), bkey(), obj(), write_reason(), p()) -> ok.
index(Index, BKey, Obj, Reason, P) ->
    WorkerName = yz_solrq:worker_regname(Index, P),
    ok = ensure_worker(Index, P),
    yz_solrq_worker:index(WorkerName, BKey, Obj, Reason, P).

%% @doc From the hash, return the registered name of a queue
-spec worker_regname(index_name(), p()) -> regname().
worker_regname(Index, Partition) ->
    make_regname("yz_solrq_worker_", Partition, Index).

make_regname(Prefix, Partition, Index) ->
    list_to_atom(Prefix ++
        integer_to_list(Partition) ++ "_" ++
        binary_to_list(Index)).

%% @doc From the hash, return the registered name of a helper
-spec helper_regname(index_name(), p()) -> regname().
helper_regname(Index, Partition) ->
    make_regname("yz_solrq_helper_", Partition, Index).

%% @doc Set the high water mark on all queues
-spec set_hwm(solrq_hwm()) -> [{{index_name(), p()},
                               {ok | error,
                                solrq_hwm() | bad_hwm_value}}].
set_hwm(HWM) ->
    [{IndexPartition, yz_solrq_worker:set_hwm(Index, Partition, HWM)} ||
        {Index, Partition} = IndexPartition <- yz_solrq_sup:active_queues()].

%% @doc Set the index parameters for all queues (note, index goes back to appenv
%%      queue is empty).
-spec set_index(index_name(), solrq_batch_min(), solrq_batch_max(),
                solrq_batch_flush_interval()) ->
                       [{{index_name(), p()}, {ok | error, {Params :: number()} |
                                  bad_index_params}}].
set_index(Index, Min, Max, DelayMsMax) ->
    [{IndexPartition, yz_solrq_worker:set_index(Index, Partition, Min, Max, DelayMsMax)} ||
        {_Index, Partition} = IndexPartition <- solrq_worker_pairs_for_index(Index)].

%% @doc Set the purge strategy on all queues
-spec set_purge_strategy(purge_strategy()) ->
                                [{{index_name(), p()}, {ok | error,
                                           purge_strategy()
                                           | bad_purge_strategy}}].
set_purge_strategy(PurgeStrategy) ->
    [{IndexPartition, yz_solrq_worker:set_purge_strategy(Index, Partition, PurgeStrategy)} ||
        {Index, Partition} =IndexPartition <- yz_solrq_sup:active_queues()].

%% @doc Request each solrq reloads from appenv - currently only affects HWM
-spec reload_appenv() -> [{{index_name(), p()}, ok}].
reload_appenv() ->
    [{IndexPartition, yz_solrq_worker:reload_appenv(Index, Partition)} ||
        {Index, Partition} = IndexPartition <- yz_solrq_sup:active_queues()].

%% @doc Signal to all Solrqs that a fuse has blown for the the specified index.
-spec blown_fuse(index_name()) -> ok.
blown_fuse(Index) ->
    lists:foreach(
        fun(Name) ->
            yz_solrq_worker:blown_fuse(Name)
        end,
        solrq_workers_for_index(Index)
    ).

%% @doc Signal to all Solrqs that a fuse has healed for the the specified index.
-spec healed_fuse(index_name()) -> ok.
healed_fuse(Index) ->
    lists:foreach(
        fun(Name) ->
            yz_solrq_worker:healed_fuse(Name)
        end,
        solrq_workers_for_index(Index)
    ).

%% @doc Return the status of all solrq workers.
-spec status() -> [{atom(), yz_solrq_worker:status()}].
status() ->
    [{Name, yz_solrq_worker:status(Name)} || Name <- solrq_worker_names()].

%% @doc Return the list of solrq names registered with this supervisor
-spec solrq_worker_names() -> [atom()].
solrq_worker_names() ->
    [yz_solrq:worker_regname(Index, Partition) ||
        {Index, Partition} <- yz_solrq_sup:active_queues()].

-spec all_solrq_workers() -> [atom()].
all_solrq_workers() ->
    [yz_solrq:worker_regname(Index, Partition) ||
        {Index, Partition} <- yz_solrq_sup:active_queues()].

-spec solrq_workers_for_partition(Partition :: p()) -> [atom()].
solrq_workers_for_partition(Partition) ->
    [yz_solrq:worker_regname(Index, Partition) ||
        {Index, WorkerPartition} <- yz_solrq_sup:active_queues(),
        Partition == WorkerPartition].

-spec solrq_worker_pairs_for_index(Index :: index_name()) -> [{index_name(), p()}].
solrq_worker_pairs_for_index(Index) ->
    [{Index, Partition} ||
        {WorkerIndex, Partition} <- yz_solrq_sup:active_queues(),
        Index == WorkerIndex].

-spec solrq_workers_for_index(Index :: index_name()) -> [atom()].
solrq_workers_for_index(Index) ->
    [yz_solrq:worker_regname(Index, Partition) ||
        {WorkerIndex, Partition} <- yz_solrq_sup:active_queues(),
        Index == WorkerIndex].

%% @doc return the total length of all solrq workers on the node.
-spec queue_total_length() -> non_neg_integer().
queue_total_length() ->
    lists:sum([yz_solrq_worker:all_queue_len(Index, Partition) || {Index, Partition} <- yz_solrq_sup:active_queues()]).

get_max_batch_size() ->
    app_helper:get_env(?YZ_APP_NAME, ?SOLRQ_BATCH_MAX, ?SOLRQ_BATCH_MAX_DEFAULT).

get_min_batch_size() ->
    app_helper:get_env(?YZ_APP_NAME, ?SOLRQ_BATCH_MIN, ?SOLRQ_BATCH_MIN_DEFAULT).

get_flush_interval() ->
    app_helper:get_env(?YZ_APP_NAME, ?SOLRQ_BATCH_FLUSH_INTERVAL,
        ?SOLRQ_BATCH_FLUSH_INTERVAL_DEFAULT).
%%%===================================================================
%%% Internal functions
%%%===================================================================

ensure_worker(Index, Partition) ->
    WorkerName = yz_solrq:worker_regname(Index, Partition),
    case whereis(WorkerName) of
        undefined ->
            %% Two processes may both get here at once. It's ok to ignore the
            %% return value here, as we would just ignore the already_started
            %% error anyway.
            ok = yz_solrq_sup:start_queue_pair(Index, Partition);
        _Pid ->
            ok
    end.
