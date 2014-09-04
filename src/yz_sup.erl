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

%% @doc Top-level supervisor for yokozuna.
%%
%% Starts no children if yokozuna is not enabled (`start_link(false)').
%%
%% Starts two sub-supervisors if yokozuna is enabled: one supervisor
%% for the JVM solr manager, and another supervisor for the rest of
%% the long-lived yokozuna processes. This top-level supervisor's
%% restart strategy is thus to allow zero restarts of its
%% sub-supervisors. If those sub-supervisors exit, something is really
%% wrong, and yokozuna should shut down.

-module(yz_sup).
-behaviour(supervisor).
-include("yokozuna.hrl").
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

init([_Enabled]) ->
    SolrSup = {yz_solr_sup,
               {yz_solr_sup, start_link, []},
               permanent, 5000, supervisor, [yz_solr_sup]},

    GeneralSup = {yz_general_sup,
                  {yz_general_sup, start_link, []},
                  permanent, infinity, supervisor, [yz_general_sup]},

    Children = [SolrSup, GeneralSup],

    %% if these sub-supervisors ever exit, there's something really
    %% wrong; don't try to restart them
    MaxR = 0,
    MaxT = 1,
    {ok, {{one_for_one, MaxR, MaxT}, Children}}.
