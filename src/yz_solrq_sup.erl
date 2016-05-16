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

-export([start_link/0, start_link/2, start_drain_fsm/1, child_count/1]).

-include("yokozuna.hrl").

-export([init/1]).

%% used only by yz_perf on resize (manual operation)
-export([child_spec/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    start_link(worker_procs(), helper_procs()).

start_link(NumQueues, NumHelpers) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [NumQueues, NumHelpers]).


%% @doc Start the drain FSM, under this supervisor
-spec start_drain_fsm(proplist()) -> {ok, pid()} | {error, Reason :: term()}.
start_drain_fsm(CallbackList) ->
    supervisor:start_child(
        ?MODULE,
        {yz_solrq_drain_fsm, {yz_solrq_drain_fsm, start_link, [CallbackList]}, temporary, 5000, worker, []}
    ).


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([NumQueues, NumHelpers]) ->
    yz_solrq:set_solrq_worker_tuple(NumQueues),
    yz_solrq:set_solrq_helper_tuple(NumHelpers),
    DrainMgrSpec = {yz_solrq_drain_mgr, {yz_solrq_drain_mgr, start_link, []}, permanent, 5000, worker, [yz_drain_mgr]},
    QueueChildren = [child_spec(worker, Name) ||
                        Name <- tuple_to_list(yz_solrq:get_solrq_worker_tuple())],
    HelperChildren = [child_spec(helper, Name) ||
                        Name <- tuple_to_list(yz_solrq:get_solrq_helper_tuple())],
    %% Using a one_for_all restart strategy as we write data to a hashed worker,
    %% which then uses a random helper to send data to Solr itself - if we want to
    %% make this one_for_one we will need to do more work monitoring the processes
    %% and responding to crashes more carefully.
    {ok, {{one_for_all, 10, 10}, [DrainMgrSpec | HelperChildren ++ QueueChildren]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

worker_procs() ->
    application:get_env(?YZ_APP_NAME, ?SOLRQ_WORKER_COUNT, 10).

helper_procs() ->
    application:get_env(?YZ_APP_NAME, ?SOLRQ_HELPER_COUNT, 10).

child_spec(helper, Name) ->
    {Name, {yz_solrq_helper, start_link, [Name]}, permanent, 5000, worker, [yz_solrq_helper]};

child_spec(worker, Name) ->
    {Name, {yz_solrq_worker, start_link, [Name]}, permanent, 5000, worker, [yz_solrq_worker]}.

-spec child_count(atom()) -> non_neg_integer().
child_count(ChildType) ->
    length([true || {_,_,_,[Type]} <- supervisor:which_children(?MODULE),
        Type == ChildType]).