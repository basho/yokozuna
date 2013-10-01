%% -------------------------------------------------------------------
%%
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

%% @doc Supervisor for the solr process manager. We want to have a
%% very different restart policy for the JVM manager than for our
%% other long-lived workers, so it is given its own supervisor.
%%
%% The strategy allows for no more than one restart in 3x the time
%% that has been set for solr startup. This is to prevent Yokozuna
%% from hanging around useless when Solr is failing to start.

-module(yz_solr_sup).
-behaviour(supervisor).
-include("yokozuna.hrl").
-export([start_link/0]).
-export([init/1]).


%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init([]) ->
    Dir = ?YZ_ROOT_DIR,
    SolrPort = yz_solr:port(),
    SolrJMXPort = yz_solr:jmx_port(),

    SolrProc = {yz_solr_proc,
                {yz_solr_proc, start_link, [Dir, SolrPort, SolrJMXPort]},
                permanent, 5000, worker, [yz_solr_proc]},

    Children = [SolrProc],

    %% if yz_solr_proc restarts more than once in 3x its startup wait
    %% time, it's probably not going to succeed on the third try
    MaxR = 1,
    MaxT = 3*yz_solr_proc:solr_startup_wait(),
    {ok, {{one_for_one, MaxR, MaxT}, Children}}.
