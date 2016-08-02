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
-module(yz_solrq).

-export([index/5, worker_regname/2, helper_regname/1,
    random_helper/0,
    num_worker_specs/0,
    num_helper_specs/0,
    set_hwm/1,
    set_index/4,
    set_purge_strategy/1,
    reload_appenv/0,
    blown_fuse/1,
    healed_fuse/1,
    solrq_worker_names/0,
    solrq_helper_names/0,
    queue_total_length/0]).

-include("yokozuna.hrl").

% exported for yz_solrq_sup
-export([set_solrq_worker_tuple/1, set_solrq_helper_tuple/1,
         get_solrq_worker_tuple/0, get_solrq_helper_tuple/0]).

% for debugging only
-export([status/0]).

-type phash() :: integer().
-type regname() :: atom().
-type size_resps() :: same_size | {shrank, non_neg_integer()} |
                      {grew, non_neg_integer()}.

-define(SOLRQS_TUPLE_KEY, solrqs_tuple).
-define(SOLRQ_HELPERS_TUPLE_KEY, solrq_helpers_tuple).

-export_type([regname/0, size_resps/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec index(index_name(), bkey(), obj(), write_reason(), p()) -> ok.
index(Index, BKey, Obj, Reason, P) ->
    WorkerName = yz_solrq:worker_regname(Index, P),
    ok = ensure_worker(WorkerName),
    yz_solrq_worker:index(WorkerName, Index, BKey, Obj, Reason, P).

%% @doc From the hash, return the registered name of a queue
-spec worker_regname(index_name(), p()) -> regname().
worker_regname(Index, Partition) ->
    make_regname("yz_solrq_worker_", Partition, Index).

make_regname(Prefix, Partition, Index) ->
    list_to_atom(Prefix ++
        integer_to_list(Partition) ++ "_" ++
        binary_to_list(Index)).

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

%% @doc return a random helper
-spec random_helper() -> regname().
random_helper() ->
    case get_solrq_helper_tuple() of
        undefined ->
            error(solrq_sup_not_started);
        Names ->
            Index = random:uniform(size(Names)),
            element(Index, Names)
    end.

%% @doc Active queue count
-spec num_worker_specs() -> non_neg_integer().
num_worker_specs() ->
    yz_solrq_sup:child_count(yz_solrq_worker).

%% @doc Active helper count
-spec num_helper_specs() -> non_neg_integer().
num_helper_specs() ->
    yz_solrq_sup:child_count(yz_solrq_helper).


%% @doc Set the high water mark on all queues
-spec set_hwm(solrq_hwm()) -> [{atom(),
                               {ok | error,
                                solrq_hwm() | bad_hwm_value}}].
set_hwm(HWM) ->
    [{Name, yz_solrq_worker:set_hwm(Name, HWM)} ||
        Name <- tuple_to_list(get_solrq_worker_tuple())].

%% @doc Set the index parameters for all queues (note, index goes back to appenv
%%      queue is empty).
-spec set_index(index_name(), solrq_batch_min(), solrq_batch_max(),
                solrq_batch_flush_interval()) ->
                       [{atom(), {ok | error, {Params :: number()} |
                                  bad_index_params}}].
set_index(Index, Min, Max, DelayMsMax) ->
    [{Name, yz_solrq_worker:set_index(Name, Index, Min, Max, DelayMsMax)} ||
        Name <- tuple_to_list(get_solrq_worker_tuple())].

%% @doc Set the purge strategy on all queues
-spec set_purge_strategy(purge_strategy()) ->
                                [{atom(), {ok | error,
                                           purge_strategy()
                                           | bad_purge_strategy}}].
set_purge_strategy(PurgeStrategy) ->
    [{Name, yz_solrq_worker:set_purge_strategy(Name, PurgeStrategy)} ||
        Name <- tuple_to_list(get_solrq_worker_tuple())].

%% @doc Request each solrq reloads from appenv - currently only affects HWM
-spec reload_appenv() -> [{atom(), ok}].
reload_appenv() ->
    [{Name, yz_solrq_worker:reload_appenv(Name)} ||
        Name <- tuple_to_list(get_solrq_worker_tuple())].

%% @doc Signal to all Solrqs that a fuse has blown for the the specified index.
-spec blown_fuse(index_name()) -> ok.
blown_fuse(Index) ->
    lists:foreach(
        fun(Name) ->
            yz_solrq_worker:blown_fuse(Name, Index)
        end,
        solrq_worker_names()
    ).

%% @doc Signal to all Solrqs that a fuse has healed for the the specified index.
-spec healed_fuse(index_name()) -> ok.
healed_fuse(Index) ->
    lists:foreach(
        fun(Name) ->
            yz_solrq_worker:healed_fuse(Name, Index)
        end,
        solrq_worker_names()
    ).

%% @doc Return the status of all solrq workers.
-spec status() -> [{atom(), yz_solrq_worker:status()}].
status() ->
    [{Name, yz_solrq_worker:status(Name)} || Name <- solrq_worker_names()].

%% @doc Return the list of solrq names registered with this supervisor
-spec solrq_worker_names() -> [atom()].
solrq_worker_names() ->
    yz_solrq_sup:active_workers().

%% @doc Return the list of solrq names registered with this supervisor
-spec solrq_helper_names() -> [atom()].
solrq_helper_names() ->
    tuple_to_list(get_solrq_helper_tuple()).

%% @doc return the total length of all solrq workers on the node.
-spec queue_total_length() -> non_neg_integer().
queue_total_length() ->
    lists:sum([yz_solrq_worker:all_queue_len(Name) || Name <- yz_solrq_sup:active_workers()]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_solrq_worker_tuple() ->
    mochiglobal:get(?SOLRQS_TUPLE_KEY).

set_solrq_worker_tuple(Size) ->
    mochiglobal:put(?SOLRQS_TUPLE_KEY, solrq_workers_tuple(Size)).

get_solrq_helper_tuple() ->
    mochiglobal:get(?SOLRQ_HELPERS_TUPLE_KEY).

set_solrq_helper_tuple(Size) ->
    mochiglobal:put(?SOLRQ_HELPERS_TUPLE_KEY, solrq_helpers_tuple(Size)).

solrq_workers_tuple(Queues) ->
    list_to_tuple([int_to_worker_regname(I) || I <- lists:seq(1, Queues)]).

solrq_helpers_tuple(Helpers) ->
    list_to_tuple([int_to_helper_regname(I) || I <- lists:seq(1, Helpers)]).

int_to_worker_regname(I) ->
    list_to_atom(lists:flatten(io_lib:format("yz_solrq_worker_~4..0b", [I]))).

int_to_helper_regname(I) ->
    list_to_atom(lists:flatten(io_lib:format("yz_solrq_helper_~4..0b", [I]))).

ensure_worker(WorkerName) ->
    case whereis(WorkerName) of
        undefined ->
            %% Two processes may both get here at once. It's ok to ignore the
            %% return value here, as we would just ignore the already_started
            %% error anyway.
            ok = yz_solrq_sup:start_worker(WorkerName);
        _Pid ->
            ok
    end.