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

%% @doc Functionality related to events.  This is the single producer of
%% writes to the ETS table `yz_events`.
%%
%% NOTE: Store the raw ring in the state because that is what is being
%%       delivered during a ring event.

-module(yz_events).
-behavior(gen_server).
-compile(export_all).
-export([code_change/3,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         init/1,
         terminate/2]).
-include("yokozuna.hrl").

-record(state, {
          previous_ring :: ring()
         }).
-define(PREV_RING(S), S#state.previous_ring).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% Callbacks
%%%===================================================================

init([]) ->
    ok = watch_ring_events(),
    ok = create_events_table(),
    ok = set_tick(),
    {ok, #state{previous_ring=yz_misc:get_ring(raw)}}.

handle_cast({ring_event, Ring}, S) ->
    PrevRing = ?PREV_RING(S),
    S2 = S#state{previous_ring=Ring},

    Previous = names(yz_index:get_indexes_from_ring(PrevRing)),
    Current = names(yz_index:get_indexes_from_ring(Ring)),
    {Removed, Added, Same} = yz_misc:delta(Previous, Current),

    PreviousFlags = flagged_buckets(PrevRing),
    CurrentFlags = flagged_buckets(Ring),
    {FlagsRemoved, FlagsAdded, _} = yz_misc:delta(PreviousFlags, CurrentFlags),

    ok = sync_indexes(Ring, Removed, Added, Same),
    %% Pass `PrevRing' because need access to index name associated
    %% with bucket.
    ok = sync_data(PrevRing, FlagsRemoved, FlagsAdded),

    {noreply, S2}.

handle_info(tick, S) ->
    %% TODO: tick and ring_event should be merged, actions taken in
    %% ring_event could fail and should be retried during tick, may
    %% need to rely on something other than ring to determine when to
    %% retry certain actions, e.g. if the `sync_data' call fails then
    %% AAE trees will be incorrect until the next ring_event or until
    %% tree rebuild
    ok = remove_non_owned_data(),

    %% Index creation may have failed during ring event.
    PrevRing = ?PREV_RING(S),
    Ring = yz_misc:get_ring(raw),
    Previous = names(yz_index:get_indexes_from_ring(PrevRing)),
    Current = names(yz_index:get_indexes_from_ring(Ring)),
    {Removed, Added, Same} = yz_misc:delta(Previous, Current),
    ok = sync_indexes(Ring, Removed, Added, Same),

    ok = set_tick(),
    {noreply, S}.

handle_call(Req, _, S) ->
    ?WARN("unexpected request ~p", [Req]),
    {noreply, S}.

code_change(_, S, _) ->
    {ok, S}.

terminate(_Reason, _S) ->
    ok = destroy_events_table().


%%%===================================================================
%%% Private
%%%===================================================================

-spec add_index(ring(), index_name()) -> ok.
add_index(Ring, Name) ->
    case yz_index:exists(Name) of
        true ->
            ok;
        false ->
            ok = yz_index:local_create(Ring, Name)
    end.

-spec add_indexes(ring(), index_set()) -> ok.
add_indexes(Ring, Names) ->
    [add_index(Ring, N) || N <- Names],
    ok.

-spec create_events_table() -> ok.
create_events_table() ->
    Opts = [named_table, protected, {read_concurrency, true}],
    ?YZ_EVENTS_TAB = ets:new(?YZ_EVENTS_TAB, Opts),
    ok.

-spec destroy_events_table() -> ok.
destroy_events_table() ->
    true = ets:delete(?YZ_EVENTS_TAB),
    ok.

-spec flagged_buckets(ring()) -> ordset(bucket()).
flagged_buckets(Ring) ->
    Buckets = riak_core_bucket:get_buckets(Ring),
    ordsets:from_list(
      [proplists:get_value(name, BProps)
       || BProps <- Buckets, yz_kv:should_index(yz_kv:get_index(BProps))]).

get_tick_interval() ->
    app_helper:get_env(?YZ_APP_NAME, tick_interval, ?YZ_DEFAULT_TICK_INTERVAL).

-spec is_unknown(tuple()) -> boolean().
is_unknown({_, {_, unknown}}) -> true;
is_unknown({_, {_, Port}}) when is_list(Port) -> false.

maybe_log({_, []}) ->
    ok;
maybe_log({Index, Removed}) ->
    ?INFO("removed non-owned partitions ~p from index ~p", [Removed, Index]).

names(Indexes) ->
    [Name || {Name,_} <- Indexes].

-spec remove_index(index_name()) -> ok.
remove_index(Name) ->
    case yz_solr:ping(Name) of
        true -> ok = yz_index:local_remove(Name);
        false -> ok
    end.

-spec remove_indexes(index_set()) -> ok.
remove_indexes(Names) ->
    [ok = remove_index(N) || N <- Names],
    ok.

%% @private
%%
%% @doc Remove documents for any data not owned by this node.
-spec remove_non_owned_data() -> ok.
remove_non_owned_data() ->
    case yz_solr:cores() of
        {ok, Cores} ->
            Indexes = ordsets:to_list(Cores),
            Removed = [{Index, yz_index:remove_non_owned_data(Index)}
                       || Index <- Indexes],
            [maybe_log(R) || R <- Removed];
        _ ->
            ok
    end,
    ok.

send_ring_event(Ring) ->
    gen_server:cast(?MODULE, {ring_event, Ring}).

set_tick() ->
    Interval = get_tick_interval(),
    erlang:send_after(Interval, ?MODULE, tick),
    ok.

-spec sync_data(ring(), list(), list()) -> ok.
sync_data(PrevRing, Removed, Added) ->
    %% TODO: check for case where index isn't added or removed, but changed
    [sync_added(Bucket) || Bucket <- Added],
    [sync_removed(PrevRing, Bucket) || Bucket <- Removed],
    ok.

-spec sync_added(bucket()) -> ok.
sync_added(Bucket) ->
    lager:info("indexing enabled for bucket ~s -- clearing AAE trees", [Bucket]),
    %% TODO: add hashtree.erl function to clear hashes for Bucket
    yz_entropy_mgr:clear_trees(),
    ok.

-spec sync_removed(ring(), bucket()) -> ok.
sync_removed(PrevRing, Bucket) ->
    lager:info("indexing disabled for bucket ~s", [Bucket]),
    Index = yz_kv:get_index(riak_core_bucket:get_bucket(Bucket, PrevRing)),
    ok = yz_solr:delete(Index, [{'query', <<?YZ_RB_FIELD_B/binary,":",Bucket/binary>>}]),
    yz_solr:commit(Index),
    yz_entropy_mgr:clear_trees(),
    ok.

sync_indexes(Ring, Removed, Added, Same) ->
    ok = remove_indexes(Removed),
    ok = add_indexes(Ring, Added ++ Same).

watch_ring_events() ->
    riak_core_ring_events:add_sup_callback(fun send_ring_event/1).
