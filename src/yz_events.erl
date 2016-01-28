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

%% @doc Functionality related to events. This is the single producer of
%%      writes to the ETS table `yz_events'.

-module(yz_events).
-behavior(gen_event).

%% API
-export([start_link/0,
         add_handler/2,
         add_callback/1,
         add_guarded_callback/1,
         add_guarded_handler/2,
         add_sup_callback/1,
         add_sup_handler/2]).

%% gen_event callbacks
-export([code_change/3,
         handle_call/2,
         handle_event/2,
         handle_info/2,
         init/1,
         terminate/2]).

%% other
-export([create_table/0]).

-include("yokozuna.hrl").

-define(NUM_TICKS_START, 1).
-define(ETS, ets_yz_events).

-record(state, {
          %% The number of ticks since the last time this value was
          %% reset to 1. This value along with
          %% `get_full_check_after/0' is used to determine when a
          %% "full check" should be performed. This is to prevent
          %% expensive checks occurring on every tick.
          num_ticks = ?NUM_TICKS_START  :: non_neg_integer(),

          %% The hash of the index cluster meta last time it was
          %% checked.
          prev_index_hash = undefined   :: term()
         }).

-define(DEFAULT_EVENTS_FULL_CHECK_AFTER, 60).
-define(DEFAULT_EVENTS_TICK_INTERVAL, 1000).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_event:start_link({local, ?MODULE}).

add_handler(Handler, Args) ->
    gen_event:add_handler(?MODULE, Handler, Args).

add_sup_handler(Handler, Args) ->
    gen_event:add_sup_handler(?MODULE, Handler, Args).

add_guarded_handler(Handler, Args) ->
    yz_eventhandler_sup:start_guarded_handler(?MODULE, Handler, Args).

add_callback(Fn) when is_function(Fn) ->
    gen_event:add_handler(?MODULE, {?MODULE, make_ref()}, [Fn]).

add_sup_callback(Fn) when is_function(Fn) ->
    gen_event:add_sup_handler(?MODULE, {?MODULE, make_ref()}, [Fn]).

add_guarded_callback(Fn) when is_function(Fn) ->
    yz_eventhandler_sup:start_guarded_handler(?MODULE, {?MODULE, make_ref()},
                                          [Fn]).

%%%===================================================================
%%% Callbacks
%%%===================================================================

init([]) ->
    ok = set_tick(),
    {ok, #state{}}.

handle_event({Index, blown}, S) ->
    handle_index_recovered(Index, down),
    {ok, S};
handle_event({Index, ok}, S) ->
    handle_index_recovered(Index, up),
    {ok, S};
handle_event({Index, removed}, S) ->
    handle_index_recovered(Index, removed),
    {ok, S};
handle_event(_Msg, S) ->
    {ok, S}.

handle_info(tick, S) ->
    PrevHash = S#state.prev_index_hash,
    CurrHash = riak_core_metadata:prefix_hash(?YZ_META_INDEXES),
    NumTicks = S#state.num_ticks,
    IsFullCheck = (NumTicks == ?NUM_TICKS_START),
    DidHashChange = PrevHash /= CurrHash,

    ok = ?MAYBE(yz_solr:is_up() andalso (IsFullCheck orelse DidHashChange),
                sync_indexes()),

    ok = ?MAYBE(IsFullCheck,
                remove_non_owned_data(yz_cover:get_ring_used())),

    ok = set_tick(),

    NumTicks2 = incr_or_wrap(NumTicks, get_full_check_after()),
    S2 = S#state{num_ticks=NumTicks2,
                 prev_index_hash=CurrHash},
    {ok, S2}.

handle_call(Req, S) ->
    ?WARN("unexpected request ~p", [Req]),
    {ok, ok, S}.

code_change(_, S, _) ->
    {ok, S}.

terminate(_Reason, _S) ->
    ok.

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Called by {@link yz_general_sup} to create the p ETS table used to
%% track registered events. Created when we add the handler after the supervisor
%% is already up for yz_events.
-spec create_table() -> ok.
create_table() ->
    _ = ets:new(?ETS, [named_table, public, set,
                          {write_concurrency, true},
                          {read_concurrency, true},
                          {keypos, 1}]),
    ok.

-spec add_index(index_name()) -> ok.
add_index(Name) ->
    case yz_index:exists(Name) of
        true -> ok;
        false -> ok = yz_index:local_create(Name)
    end,
    yz_fuse:create(Name).

-spec add_indexes(index_set()) -> ok.
add_indexes(Names) ->
    _ = [ok = add_index(N) || N <- Names],
    ok.

%% @private
%%
%% @doc Get the number of ticks after which a full check is
%% performed. If the tick interval is 1 second and this function
%% returns 60 then a full check will be performed every 60 seconds.
-spec get_full_check_after() -> non_neg_integer().
get_full_check_after() ->
    NumTicks = app_helper:get_env(?YZ_APP_NAME, events_full_check_after,
                                  ?DEFAULT_EVENTS_FULL_CHECK_AFTER),
    case is_integer(NumTicks) of
        true -> NumTicks;
        false -> 60
    end.

%% @private
%%
%% @doc Return the tick interval specified in milliseconds.
-spec get_tick_interval() -> ms().
get_tick_interval() ->
    I = app_helper:get_env(?YZ_APP_NAME, events_tick_interval,
                           ?DEFAULT_EVENTS_TICK_INTERVAL),
    case is_integer(I) andalso I >= 1000 of
        true -> I;
        false -> 1000
    end.

%% @private
%%
%% @doc Increment the integer N unless it is equal to M then wrap
%% around to 0.
-spec incr_or_wrap(non_neg_integer(), non_neg_integer()) -> non_neg_integer().
incr_or_wrap(N, M) ->
    case N == M of
        true -> ?NUM_TICKS_START;
        false -> N + 1
    end.

maybe_log({_, []}) ->
    ok;
maybe_log({Index, Removed}) ->
    ?INFO("removed non-owned partitions ~p from index ~p", [Removed, Index]).

-spec remove_index(index_name()) -> ok.
remove_index(Name) ->
    case yz_solr:ping(Name) of
        true ->
            ok = yz_index:local_remove(Name),
            yz_fuse:remove(Name);
        _ -> ok
    end.

-spec remove_indexes(index_set()) -> ok.
remove_indexes(Names) ->
    _ = [ok = remove_index(N) || N <- Names],
    ok.

%% @private
%%
%% @doc Remove documents for any data not owned by this node.
-spec remove_non_owned_data(ring() | unkown) -> ok.
remove_non_owned_data(unknown) ->
    %% The ring used to calculate the current coverage plan could not
    %% be determined. In this case do nothing to prevent removing data
    %% that the current coverage plan is using.
    ?DEBUG("the current ring is unknown, no data can be removed", []),
    ok;
remove_non_owned_data(Ring) ->
    case yz_solr:cores() of
        {ok, Cores} ->
            Indexes = ordsets:to_list(Cores),
            Removed = [{Index, yz_index:remove_non_owned_data(Index, Ring)}
                       || Index <- Indexes],
            [maybe_log(R) || R <- Removed];
        _ ->
            ok
    end,
    ok.

set_tick() ->
    Interval = get_tick_interval(),
    erlang:send_after(Interval, ?MODULE, tick),
    ok.

%% @private
%%
%% @doc Synchronize the Solr indexes with the official list stored in
%% memory.
-spec sync_indexes() -> ok.
sync_indexes() ->
    case yz_solr:cores() of
        {ok, IndexesFromSolr} ->
            IndexSetFromSolr = ordsets:from_list(IndexesFromSolr),
            IndexSetFromMeta = ordsets:from_list(
                                 yz_index:get_indexes_from_meta()),
            {Removed, Added, Same} = yz_misc:delta(IndexSetFromSolr,
                                                   IndexSetFromMeta),
            case {Removed, Added} of
                {[], []} -> ok;
                _ ->
                    lager:info("Delta: Removed: ~p Added: ~p Same: ~p",
                        [Removed, Added, Same])
            end,
            ok = sync_indexes(Removed, Added, Same);
        {error, _Reason} ->
            ok
    end.

%% @private
%%
%% @doc Synchronize Solr indexes by removing the indexes in the
%% `Removed' set and creating, if they don't already exist, those
%% indexes in the `Added' and `Same' set.
%%
%% @see sync_indexes/0
-spec sync_indexes(index_set(), index_set(), index_set()) -> ok.
sync_indexes(Removed, Added, Same) ->
    ok = remove_indexes(Removed),
    ok = add_indexes(Added ++ Same).

%% @private
%% @doc Check and update `yz_events' ETS if the index has recovered from it's
%%      fuse being blown or has been reset/removed.
-spec handle_index_recovered(index_name(), down|removed|up) -> true.
handle_index_recovered(Index, down) ->
    ets:insert(?ETS, {Index, {state, down}});
handle_index_recovered(Index, removed) ->
    ets:delete(?ETS, Index);
handle_index_recovered(Index, up) ->
    Recovered = ets:lookup(?ETS, Index),
    case proplists:get_value(Index, Recovered, []) of
        {state, down} ->
            yz_stat:fuse_recovered(Index),
            ets:insert(?ETS, {Index, {state, up}});
        _ ->
            ets:insert(?ETS, {Index, {state, up}})
    end.
