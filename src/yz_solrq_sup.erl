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

-export([start_link/0, regname/1]).
-export([init/1]).

-define(SOLRQS_TUPLE_KEY, solrqs_tuple).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% From the hash, return the registered name of a queue
regname(Hash) ->
    case mochiglobal:get(?SOLRQS_TUPLE_KEY) of
        undefined ->
            error(solrq_sup_not_started);
        Names ->
            Index = 1 + (Hash rem size(Names)),
            element(Index, Names)
    end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }}).
init([]) ->
    SolrQs = solrqs_tuple(),
    mochiglobal:put(?SOLRQS_TUPLE_KEY, SolrQs),
    Children = [{Name, {yz_solrq, start_link, [Name]},
                 permanent, 5000, worker, [yz_solrq]} ||
                   Name <- tuple_to_list(SolrQs)],
    {ok, {{one_for_one, 10, 10}, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

solrqs_tuple() ->
    NumSolrQ =application:get_env(yokozuna, num_solrq, 10),
    list_to_tuple([int_to_regname(I) || I <- lists:seq(1, NumSolrQ)]).

int_to_regname(I) ->
    list_to_atom(lists:flatten(io_lib:format("yz_solrq_~4..0b", [I]))).
