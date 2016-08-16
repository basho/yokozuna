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
-module(yz_solrq_sup).

-behaviour(supervisor).

-export([start_link/0, start_drain_fsm/1, child_count/1, start_worker/2, active_workers/0, sync_active_queue_pairs/0]).

-include("yokozuna.hrl").

-export([init/1]).


%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%% @doc Start the drain FSM, under this supervisor
-spec start_drain_fsm(proplist()) -> {ok, pid()} | {error, Reason :: term()}.
start_drain_fsm(Parameters) ->
    PartitionToDrain = proplists:get_value(
        ?DRAIN_PARTITION, Parameters, undefined
    ),
    supervisor:start_child(
        ?MODULE,
        {PartitionToDrain, {yz_solrq_drain_fsm, start_link, [Parameters]}, temporary, 5000, worker, []}
    ).
-spec start_worker(Index::index_name(), Partition::p()) -> ok.
start_worker(Index, Partition) ->
    validate_child_started(
        supervisor:start_child(?MODULE, queue_pair_spec({Index, Partition}))).

active_workers() ->
    AllChildren = supervisor:which_children(yz_solrq_sup),
    PairSups = [SupPid || {_IndexPartition, SupPid, _Type, Modules} <- AllChildren, Modules == [yz_solrq_queue_pair_sup]],
    PairChildren = lists:flatten([supervisor:which_children(Sup) || Sup <- PairSups]),
    Workers = [{Index, Partition} || {{worker, Index, Partition}, _Child, _Type, Modules} <- PairChildren, Modules == [yz_solrq_worker]],
    Workers.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->

    DrainMgrSpec = {yz_solrq_drain_mgr, {yz_solrq_drain_mgr, start_link, []}, permanent, 5000, worker, [yz_drain_mgr]},

    QueueChildren = [queue_pair_spec(IndexPartition) ||
                        IndexPartition <- required_solrq_workers()],
    %% Using a one_for_all restart strategy as we write data to a hashed worker,
    %% which then uses a random helper to send data to Solr itself - if we want to
    %% make this one_for_one we will need to do more work monitoring the processes
    %% and responding to crashes more carefully.
    {ok, {{one_for_all, 10, 10}, [DrainMgrSpec | QueueChildren]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec child_count(atom()) -> non_neg_integer().
child_count(ChildType) ->
    length([true || {_,_,_,[Type]} <- supervisor:which_children(?MODULE),
        Type == ChildType]).

validate_child_started({ok, _Child}) ->
    ok;
validate_child_started({ok, _Child, _Info}) ->
    ok;
validate_child_started({error, already_present}) ->
    ok;
validate_child_started({error, {already_started, _Child}}) ->
    ok;
validate_child_started(Error) ->
    throw(Error).

required_solrq_workers() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Partitions = riak_core_ring:my_indices(Ring),
    Indexes = yz_index:get_indexes_from_meta(),
    [{Index, Partition} ||
        Partition <- Partitions,
        Index <- Indexes].
%% TODO: we shouldn't need ?YZ_INDEX_TOMBSTONE if we just update the YZ AAE tree
%% when we call index rather than pushing the value all the way to the solrq
        %%Index =/= ?YZ_INDEX_TOMBSTONE].

sync_active_queue_pairs() ->
    ActiveWorkers = active_workers(),
    RequiredWorkers = required_solrq_workers(),
    WorkersToStop = ActiveWorkers -- RequiredWorkers,
    lists:foreach(fun({Index, Partition}) -> yz_solrq_worker:stop(Index, Partition) end, WorkersToStop),
    MissingWorkers = RequiredWorkers -- ActiveWorkers,
    lists:foreach(fun({Index, Partition}) -> start_worker(Index, Partition) end, MissingWorkers),
    ok.

queue_pair_spec({Index, Partition} = Id) ->
    Id = {Index, Partition},
    {Id, {yz_solrq_queue_pair_sup, start_link, [Index, Partition]}, permanent, 5000, worker, [yz_solrq_queue_pair_sup]}.
