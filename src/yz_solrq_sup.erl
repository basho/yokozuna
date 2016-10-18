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

-export([start_link/0, start_drain_fsm/1, child_count/1, start_queue_pair/2, active_queues/0, sync_active_queue_pairs/0]).

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
-spec start_queue_pair(Index::index_name(), Partition::p()) -> ok.
start_queue_pair(Index, Partition) ->
    lager:info(
        "Starting solrq supervisor for index ~p and partition ~p",
        [Index, Partition]
    ),
    validate_child_started(
        supervisor:start_child(?MODULE, queue_pair_spec({Index, Partition}))).

-spec active_queues() -> [{index_name(), p()}].
active_queues() ->
    PairSups = find_pair_supervisors(),
    PairChildren = lists:flatten([supervisor:which_children(Sup) || Sup <- PairSups]),
    Workers = [{Index, Partition} ||
               {{worker, Index, Partition}, _Child, _Type, Modules} <- PairChildren,
               Modules == [yz_solrq_worker]],
    Workers.

-spec find_pair_supervisors() -> [PairSupPid::pid()].
find_pair_supervisors() ->
    AllChildren = supervisor:which_children(yz_solrq_sup),
    PairSups = [SupPid ||
                {_IndexPartition, SupPid, _Type, Modules} <- AllChildren,
                Modules == [yz_solrq_queue_pair_sup]],
    PairSups.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->

    DrainMgrSpec = {yz_solrq_drain_mgr, {yz_solrq_drain_mgr, start_link, []}, permanent, 5000, worker, [yz_drain_mgr]},

    QueuePairSupervisors = [queue_pair_spec(IndexPartition) ||
                        IndexPartition <- required_queues()],
    {ok, {{one_for_one, 10, 10}, [DrainMgrSpec | QueuePairSupervisors]}}.

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

required_queues() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Partitions = yz_misc:owned_and_next_partitions(node(), Ring),
    Indexes = yz_index:get_indexes_from_meta(),
    [{Index, Partition} ||
        Partition <- ordsets:to_list(Partitions),
        Index <- Indexes].
%% TODO: we shouldn't need ?YZ_INDEX_TOMBSTONE if we just update the YZ AAE tree
%% when we call index rather than pushing the value all the way to the solrq
        %%Index =/= ?YZ_INDEX_TOMBSTONE].

sync_active_queue_pairs() ->
    ActiveQueues = active_queues(),
    RequiredQueues = required_queues(),
    QueuePairsToStop = ActiveQueues -- RequiredQueues,
    lists:foreach(fun({Index, Partition}) -> stop_queue_pair(Index, Partition) end, QueuePairsToStop),
    MissingWorkers = RequiredQueues -- ActiveQueues,
    lists:foreach(fun({Index, Partition}) -> start_queue_pair(Index, Partition) end, MissingWorkers),
    ok.

stop_queue_pair(Index, Partition) ->
    lager:info(
        "Stopping solrq supervisor for index ~p and partition ~p",
        [Index, Partition]
    ),
    SupId = {Index, Partition},
    case supervisor:terminate_child(?MODULE, SupId) of
        ok ->
            _ = supervisor:delete_child(?MODULE, SupId);
        _ ->
            ok
    end.

queue_pair_spec({Index, Partition} = Id) ->
    Id = {Index, Partition},
    {Id, {yz_solrq_queue_pair_sup, start_link, [Index, Partition]}, permanent, 5000, supervisor, [yz_solrq_queue_pair_sup]}.
