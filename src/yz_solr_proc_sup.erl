%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(yz_solr_proc_sup).
-behavior(supervisor).
-include("yokozuna.hrl").
-compile(export_all).
-export([init/1,
         start_link/0]).


%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_Args) ->
    Dir = ?YZ_ROOT_DIR,
    SolrPort = yz_solr:port(),
    SolrJMXPort = yz_solr:jmx_port(),
    SolrProc = {yz_solr_proc,
                {yz_solr_proc, start_link, [Dir, SolrPort, SolrJMXPort]},
                permanent, 5000, worker, [yz_solr_proc]},

    {ok, {{one_for_one, 5, 60}, [SolrProc]}}.
