%% -------------------------------------------------------------------
%% Copyright (c) 2016 Basho Technologies, Inc. All Rights Reserved.
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
-module(yz_solrq_queue_pair_sup).

-include("yokozuna.hrl").

-behaviour(supervisor).

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(index_name(), p()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Index, Partition) ->
    supervisor:start_link(?MODULE, [Index, Partition]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }} |
    ignore).
init([Index, Partition]) ->
    RestartStrategy = one_for_all,
    MaxRestarts = 10,
    MaxSecondsBetweenRestarts = 10,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    HelperChild = helper_spec(Index, Partition),
    WorkerChild = worker_spec(Index, Partition),

    {ok, {SupFlags, [HelperChild, WorkerChild]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


helper_spec(Index, Partition) ->
    Id = {helper, Index, Partition},
    {Id, {yz_solrq_helper, start_link, [Index, Partition]}, permanent, 5000, worker, [yz_solrq_helper]}.

worker_spec(Index, Partition) ->
    Id = {worker, Index, Partition},
    {Id, {yz_solrq_worker, start_link, [Index, Partition]}, permanent, 5000, worker, [yz_solrq_worker]}.
