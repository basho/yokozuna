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
         reload_appenv/0,
         blown_fuse/1,
         healed_fuse/1,
         solrq_names/0,
         solrq_helper_names/0,
         start_drain_fsm/1]).

-include("yokozuna.hrl").

-export([init/1]).
% exported for testing
-export([set_solrq_tuple/1, set_solrq_helper_tuple/1]).

-type phash() :: integer().
-type regname() :: atom().
-type size_resps() :: same_size | {shrank, non_neg_integer()} |
                      {grew, non_neg_integer()}.

-define(SOLRQS_TUPLE_KEY, solrqs_tuple).
-define(SOLRQ_HELPERS_TUPLE_KEY, solrq_helpers_tuple).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    start_link(queue_procs(), helper_procs()).

start_link(NumQueues, NumHelpers) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [NumQueues, NumHelpers]).

%% @doc From the hash, return the registered name of a queue
-spec queue_regname(phash()) -> regname().
queue_regname(Hash) ->
    case get_solrq_tuple() of
        undefined ->
            error(solrq_sup_not_started);
        Names ->
            Index = 1 + (Hash rem size(Names)),
            element(Index, Names)
    end.

%% @doc From the hash, return the registered name of a helper
-spec helper_regname(phash()) -> regname().
helper_regname(Hash) ->
    case get_solrq_helper_tuple() of
        undefined ->
            error(solrq_sup_not_started);
        Names ->
            Index = 1 + (Hash rem size(Names)),
            element(Index, Names)
    end.

%% @doc Active queue count
-spec num_queue_specs() -> non_neg_integer().
num_queue_specs() ->
    child_count(yz_solrq).

%% @doc Active helper count
-spec num_helper_specs() -> non_neg_integer().
num_helper_specs() ->
    child_count(yz_solrq_helper).

%% @doc Resize the number of queues. For debugging/testing only,
%%      this will briefly cause the worker that queues remap to
%%      to change so updates may be out of order briefly.
-spec resize_queues(pos_integer()) -> size_resps().
resize_queues(NewSize) when NewSize > 0 ->
    do_child_resize(NewSize, num_queue_specs(),
        fun set_solrq_tuple/1,
        fun int_to_queue_regname/1,
        fun helper_child/1).

%% @doc Resize the number of helpers. For debugging/testing only,
%%      this will briefly cause the worker that queues remap to
%%      to change so updates may be out of order briefly.
-spec resize_helpers(pos_integer()) -> size_resps().
resize_helpers(NewSize) when NewSize > 0 ->
    do_child_resize(NewSize, num_helper_specs(),
        fun set_solrq_helper_tuple/1,
        fun int_to_helper_regname/1,
        fun helper_child/1).

%% @doc Set the high water mark on all queues
-spec set_hwm(non_neg_integer()) -> [{index_name, {ok, non_neg_integer()}}].
set_hwm(HWM) ->
    [{Name, catch yz_solrq:set_hwm(Name, HWM)} ||
        Name <- tuple_to_list(get_solrq_tuple())].

%% @doc Set the index parameters for all queues (note, index goes back to appenv
%%      queue is empty).
-spec set_index(index_name(), non_neg_integer(), non_neg_integer(),
                non_neg_integer()) -> [{index_name(),
                                       tuple(Params :: non_neg_integer())}].
set_index(Index, Min, Max, DelayMsMax) ->
    [{Name, catch yz_solrq:set_index(Name, Index, Min, Max, DelayMsMax)} ||
        Name <- tuple_to_list(get_solrq_tuple())].

%% @doc Request each solrq reloads from appenv - currently only affects HWM
-spec reload_appenv() -> [{index_name(), ok}].
reload_appenv() ->
    [{Name, catch yz_solrq:reload_appenv(Name)} ||
        Name <- tuple_to_list(get_solrq_tuple())].

%% @doc Signal to all Solrqs that a fuse has blown for the the specified index.
-spec blown_fuse(index_name()) -> ok.
blown_fuse(Index) ->
    lists:foreach(
        fun(Name) ->
            yz_solrq:blown_fuse(Name, Index)
        end,
        tuple_to_list(get_solrq_tuple())
    ).

%% @doc Signal to all Solrqs that a fuse has healed for the the specified index.
-spec healed_fuse(index_name()) -> ok.
healed_fuse(Index) ->
    lists:foreach(
        fun(Name) ->
            yz_solrq:healed_fuse(Name, Index)
        end,
        tuple_to_list(get_solrq_tuple())
    ).

%% @doc Return the list of solrq names registered with this supervisor
-spec solrq_names() -> [atom()].
solrq_names() ->
    tuple_to_list(get_solrq_tuple()).

%% @doc Return the list of solrq names registered with this supervisor
-spec solrq_helper_names() -> [atom()].
solrq_helper_names() ->
    tuple_to_list(get_solrq_helper_tuple()).


%% @doc Start the drain supervsior, under this supervisor
-spec start_drain_fsm(fun(() -> ok)) -> {ok, pid()} | {error, term()}.
start_drain_fsm(DrainCompleteCallback) ->
    supervisor:start_child(
        ?MODULE,
        {yz_solrq_drain_fsm, {yz_solrq_drain_fsm, start_link, [DrainCompleteCallback]}, temporary, 5000, worker, []}
    ).


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
    DrainMgrSpec = {yz_solrq_drain_mgr, {yz_solrq_drain_mgr, start_link, []}, permanent, 5000, worker, [yz_drain_mgr]},
    QueueChildren = [queue_child(Name) ||
                        Name <- tuple_to_list(get_solrq_tuple())],
    HelperChildren = [helper_child(Name) ||
                        Name <- tuple_to_list(get_solrq_helper_tuple())],
    {ok, {{one_for_all, 10, 10}, [DrainMgrSpec | HelperChildren ++ QueueChildren]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec child_count(atom()) -> non_neg_integer().
child_count(ChildType) ->
    length([true || {_,_,_,[Type]} <- supervisor:which_children(?MODULE),
                   Type == ChildType]).

-spec do_child_resize(pos_integer(), non_neg_integer(),
                      fun((pos_integer()) -> ok),
                      fun((non_neg_integer()) -> regname()),
                      fun((regname()) -> {regname(), pid()})) -> size_resps().
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
