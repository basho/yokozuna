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
-module(yz_solrq_helper_sup).

-behaviour(supervisor).

-export([start_link/0,  start_link/1, regname/1, resize/1, set_hwm/1]).
-export([init/1]).

-define(SOLRQ_HELPERS_TUPLE_KEY, solrq_helpers_tuple).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    start_link(num_workers()).

start_link(NumWorkers) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [NumWorkers]).

%% From the hash, return the registered name of a queue
regname(Hash) ->
    case mochiglobal:get(?SOLRQ_HELPERS_TUPLE_KEY) of
        undefined ->
            error(solrq_sup_not_started);
        Names ->
            Index = 1 + (Hash rem size(Names)),
            element(Index, Names)
    end.

%% Resize the number of queues.  For debugging/testing only,
%% this will briefly cause the worker that queues remap to
%% to change so updates may be out of order briefly.
resize(NewSize) when NewSize > 0 ->
    OldSize =  proplists:get_value(workers, supervisor:count_children(?MODULE)),
    %% Shrink to single worker while we mess with the
    %% running workers
    Result =
        case NewSize of
            OldSize ->
                same_size;
            NewSize when NewSize < OldSize ->
                %% Reduce down to the new size before killing
                mochiglobal:put(?SOLRQ_HELPERS_TUPLE_KEY, solrq_helpers_tuple(NewSize)),
                _ = [begin
                         Name = int_to_regname(I),
                         _ = supervisor:terminate_child(?MODULE, Name),
                         ok = supervisor:delete_child(?MODULE, Name)
                     end || I <- lists:seq(NewSize + 1, OldSize)],
                {shrank, OldSize - NewSize};
            NewSize when NewSize > OldSize ->
                [supervisor:start_child(?MODULE, make_child(int_to_regname(I))) ||
                    I <- lists:seq(OldSize + 1, NewSize)],
                mochiglobal:put(?SOLRQ_HELPERS_TUPLE_KEY, solrq_helpers_tuple(NewSize)),
                {grew, NewSize - OldSize}
        end,
    Result.

set_hwm(HWM) ->
    [{Name, catch yz_solrq:set_hwm(Name, HWM)} ||
        Name <- tuple_to_list(mochiglobal:get(?SOLRQ_HELPERS_TUPLE_KEY))].

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }}).
init([NumWorkers]) ->
    SolrQHelpers = solrq_helpers_tuple(NumWorkers),
    mochiglobal:put(?SOLRQ_HELPERS_TUPLE_KEY, SolrQHelpers),
    Children = [make_child(Name) ||
                   Name <- tuple_to_list(SolrQHelpers)],
    {ok, {{one_for_one, 10, 10}, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

num_workers() ->
    application:get_env(yokozuna, num_solrq_helpers, 10).

solrq_helpers_tuple(NumSolrQ) ->
    list_to_tuple([int_to_regname(I) || I <- lists:seq(1, NumSolrQ)]).

int_to_regname(I) ->
    list_to_atom(lists:flatten(io_lib:format("yz_solrq_helper_~4..0b", [I]))).

make_child(Name) ->
    {Name, {yz_solrq_helper, start_link, [Name]}, permanent, 5000, worker, [yz_solrq_helper]}.
