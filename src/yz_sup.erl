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

-module(yz_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).


%%%===================================================================
%%% API
%%%===================================================================

start_link(Enabled) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Enabled]).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init([false]) ->
    %% Yokozuna is disabled, start a supervisor without any children.
    {ok, {{one_for_one, 5, 10}, []}};

init([Enabled]) ->
    SolrProc = {yz_solr_proc_sup,
                {yz_solr_proc_sup, start_link, []},
                permanent, infinity, supervisor, [yz_solr_proc_sup]},
    Events = {yz_events,
              {yz_events, start_link, []},
              permanent, 5000, worker, [yz_events]},

    HashtreeSup = {yz_index_hashtree_sup,
                   {yz_index_hashtree_sup, start_link, []},
                   permanent, infinity, supervisor, [yz_index_hashtree_sup]},

    EntropyMgr = {yz_entropy_mgr,
                  {yz_entropy_mgr, start_link, []},
                  permanent, 5000, worker, [yz_entropy_mgr]},

    Cover = {yz_cover,
             {yz_cover, start_link, []},
             permanent, 5000, worker, [yz_cover]},

    Children = [SolrProc, Events, HashtreeSup, EntropyMgr, Cover],

    {ok, {{one_for_one, 5, 10}, Children}}.
