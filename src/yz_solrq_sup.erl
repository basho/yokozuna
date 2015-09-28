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

-export([start_link/0, start_link/2,
         queue_regname/1, helper_regname/1,
         num_queue_specs/0, resize_queues/1,
         num_helper_specs/0, resize_helpers/1,
         set_hwm/1,
         set_index/4,
         reload_appenv/0]).
-export([init/1]).
-export([set_solrq_tuple/1, set_solrq_helper_tuple/1]). % exported for testing

-define(SOLRQS_TUPLE_KEY, solrqs_tuple).
-define(SOLRQ_HELPERS_TUPLE_KEY, solrq_helpers_tuple).

%%%===================================================================
%%% API functions
%%%===================================================================

%% -spec(start_link() ->
%%     {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    start_link(queue_procs(), helper_procs()).

start_link(NumQueues, NumHelpers) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [NumQueues, NumHelpers]).

%% From the hash, return the registered name of a queue
queue_regname(Hash) ->
    case get_solrq_tuple() of
        undefined ->
            error(solrq_sup_not_started);
        Names ->
            Index = 1 + (Hash rem size(Names)),
            element(Index, Names)
    end.

%% From the hash, return the registered name of a helper
helper_regname(Hash) ->
    case get_solrq_helper_tuple() of
        undefined ->
            error(solrq_sup_not_started);
        Names ->
            Index = 1 + (Hash rem size(Names)),
            element(Index, Names)
    end.

%% Active queue count
num_queue_specs() ->
    child_count(yz_solrq).

%% Active helper count
num_helper_specs() ->
    child_count(yz_solrq_helper).

%% Resize the number of queues.  For debugging/testing only,
%% this will briefly cause the worker that queues remap to
%% to change so updates may be out of order briefly.
resize_queues(NewSize) when NewSize > 0 ->
    do_child_resize(NewSize, num_queue_specs(),
        fun set_solrq_tuple/1,
        fun int_to_queue_regname/1,
        fun helper_child/1).

%% Resize the number of helpers.  For debugging/testing only,
%% this will briefly cause the worker that queues remap to
%% to change so updates may be out of order briefly.
resize_helpers(NewSize) when NewSize > 0 ->
    do_child_resize(NewSize, num_helper_specs(),
        fun set_solrq_helper_tuple/1,
        fun int_to_helper_regname/1,
        fun helper_child/1).

%% Set the high water mark on all queues
set_hwm(HWM) ->
    [{Name, catch yz_solrq:set_hwm(Name, HWM)} ||
        Name <- tuple_to_list(get_solrq_tuple())].

%% Set the index parameters for all queues (note, index goes back to appenv
%% queue is empty).
set_index(Index, Min, Max, DelayMsMax) ->
    [{Name, catch yz_solrq:set_index(Name, Index, Min, Max, DelayMsMax)} ||
        Name <- tuple_to_list(get_solrq_tuple())].

%% Request each solrq reloads from appenv - currently only affects HWM
reload_appenv() ->
    [{Name, catch yz_solrq:reload_appenv(Name)} ||
        Name <- tuple_to_list(get_solrq_tuple())].


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }}).

init([NumQueues, NumHelpers]) ->
    set_solrq_tuple(NumQueues),
    set_solrq_helper_tuple(NumHelpers),
    QueueChildren = [queue_child(Name) ||
                        Name <- tuple_to_list(get_solrq_tuple())],
    HelperChildren = [helper_child(Name) ||
                        Name <- tuple_to_list(get_solrq_helper_tuple())],
    {ok, {{one_for_all, 10, 10}, HelperChildren ++ QueueChildren}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

child_count(ChildType) ->
    length([true || {_,_,_,[Type]} <- supervisor:which_children(?MODULE), Type == ChildType]).

do_child_resize(NewSize, OldSize, SetTupleFun, RegnameFun, ChildSpecFun) ->
    case NewSize of
        OldSize ->
            same_size;
        NewSize when NewSize < OldSize ->
            %% Reduce down to the new size before killing
            SetTupleFun(NewSize),
            _ = [begin
                     Name = RegnameFun(I),
                     _ = supervisor:terminate_child(?MODULE, Name),
                     ok = supervisor:delete_child(?MODULE, Name)
                 end || I <- lists:seq(NewSize + 1, OldSize)],
            {shrank, OldSize - NewSize};
        NewSize when NewSize > OldSize ->
            [supervisor:start_child(?MODULE, ChildSpecFun(RegnameFun(I))) ||
                I <- lists:seq(OldSize + 1, NewSize)],
            SetTupleFun(NewSize),
            {grew, NewSize - OldSize}
    end.

get_solrq_tuple() ->
    mochiglobal:get(?SOLRQS_TUPLE_KEY).

set_solrq_tuple(Size) ->
    mochiglobal:put(?SOLRQS_TUPLE_KEY, solrqs_tuple(Size)).

get_solrq_helper_tuple() ->
    mochiglobal:get(?SOLRQ_HELPERS_TUPLE_KEY).

set_solrq_helper_tuple(Size) ->
    mochiglobal:put(?SOLRQ_HELPERS_TUPLE_KEY, solrq_helpers_tuple(Size)).

queue_procs() ->
    application:get_env(yokozuna, num_solrq, 10).

helper_procs() ->
    application:get_env(yokozuna, num_solrq_helpers, 10).

solrqs_tuple(Queues) ->
    list_to_tuple([int_to_queue_regname(I) || I <- lists:seq(1, Queues)]).

solrq_helpers_tuple(Helpers) ->
    list_to_tuple([int_to_helper_regname(I) || I <- lists:seq(1, Helpers)]).

int_to_queue_regname(I) ->
    list_to_atom(lists:flatten(io_lib:format("yz_solrq_~4..0b", [I]))).

int_to_helper_regname(I) ->
    list_to_atom(lists:flatten(io_lib:format("yz_solrq_helper_~4..0b", [I]))).

queue_child(Name) ->
    {Name, {yz_solrq, start_link, [Name]}, permanent, 5000, worker, [yz_solrq]}.

helper_child(Name) ->
    {Name, {yz_solrq_helper, start_link, [Name]}, permanent, 5000, worker, [yz_solrq_helper]}.
