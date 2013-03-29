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

-spec get_mapping() -> list().
get_mapping() ->
    case ets:lookup(?YZ_EVENTS_TAB, mapping) of
        [{mapping, Mapping}] -> Mapping;
        [] -> []
    end.

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

handle_cast({ring_event, Ring}=RE, S) ->
    PrevRing = ?PREV_RING(S),
    S2 = S#state{previous_ring=Ring},
    Mapping = get_mapping(),
    Mapping2 = new_mapping(RE, Mapping),
    ok = set_mapping(Mapping2),

    Previous = names(yz_index:get_indexes_from_ring(PrevRing)),
    Current = names(yz_index:get_indexes_from_ring(Ring)),
    {Removed, Added, Same} = yz_misc:delta(Previous, Current),

    PreviousFlags = flagged_buckets(PrevRing),
    CurrentFlags = flagged_buckets(Ring),
    {FlagsRemoved, FlagsAdded, _} = yz_misc:delta(PreviousFlags, CurrentFlags),

    ok = sync_indexes(Ring, Removed, Added, Same),
    ok = sync_data(FlagsRemoved, FlagsAdded),

    {noreply, S2}.

handle_info(tick, S) ->
    ok = remove_non_owned_data(),

    Mapping = get_mapping(),
    Mapping2 = check_unkown(Mapping),
    ok = set_mapping(Mapping2),

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

-spec add_node(node(), list()) -> list().
add_node(Node, Mapping) ->
    HostPort = host_port(Node),
    lists:keystore(Node, 1, Mapping, {Node, HostPort}).

-spec add_nodes([node()], list()) -> list().
add_nodes(Nodes, Mapping) ->
    lists:foldl(fun add_node/2, Mapping, Nodes).

-spec check_unkown(list()) -> list().
check_unkown(Mapping) ->
    Unknown = lists:filter(fun is_unknown/1, Mapping),
    add_nodes(just_nodes(Unknown), Mapping).

-spec create_events_table() -> ok.
create_events_table() ->
    Opts = [named_table, protected, {read_concurrency, true}],
    ?YZ_EVENTS_TAB = ets:new(?YZ_EVENTS_TAB, Opts),
    ok.

-spec destroy_events_table() -> ok.
destroy_events_table() ->
    true = ets:delete(?YZ_EVENTS_TAB),
    ok.

flagged_buckets(Ring) ->
    Buckets = riak_core_bucket:get_buckets(Ring),
    [proplists:get_value(name, BProps)
     || BProps <- Buckets, proplists:get_bool(?YZ_INDEX_CONTENT, BProps)].

get_tick_interval() ->
    app_helper:get_env(?YZ_APP_NAME, tick_interval, ?YZ_DEFAULT_TICK_INTERVAL).

-spec host_port(node()) -> {string(), non_neg_integer() | unknown}.
host_port(Node) ->
    case rpc:call(Node, yz_solr, port, [], 5000) of
        {badrpc, Reason} ->
            ?ERROR("error retrieving Solr port ~p ~p", [Node, Reason]),
            {hostname(Node), unknown};
        Port when is_list(Port) ->
            {hostname(Node), Port}
    end.

-spec hostname(node()) -> string().
hostname(Node) ->
    S = atom_to_list(Node),
    [_, Host] = re:split(S, "@", [{return, list}]),
    Host.

-spec is_unknown(tuple()) -> boolean().
is_unknown({_, {_, unknown}}) -> true;
is_unknown({_, {_, Port}}) when is_list(Port) -> false.

-spec just_nodes(list()) -> [node()].
just_nodes(Mapping) ->
    [Node || {Node, _} <- Mapping].

maybe_log({_, []}) ->
    ok;
maybe_log({Index, Removed}) ->
    ?INFO("removed non-owned partitions ~p from index ~p", [Removed, Index]).

names(Indexes) ->
    [Name || {Name,_} <- Indexes].

-spec new_mapping(event(), list()) -> list().
new_mapping({ring_event, Ring}, Mapping) ->
    Nodes = riak_core_ring:all_members(Ring),
    {Removed, Added} = node_ops(Mapping, Nodes),
    Mapping2 = remove_nodes(Removed, Mapping),
    Mapping3 = add_nodes(Added, Mapping2),
    check_unkown(Mapping3).

-spec node_ops(list(), list()) -> {Removed::list(), Added::list()}.
node_ops(Mapping, Nodes) ->
    MappingNodesSet = sets:from_list(just_nodes(Mapping)),
    NodesSet = sets:from_list(Nodes),
    Removed = sets:subtract(MappingNodesSet, NodesSet),
    Added = sets:subtract(NodesSet, MappingNodesSet),
    {sets:to_list(Removed), sets:to_list(Added)}.

-spec remove_index(index_name()) -> ok.
remove_index(Name) ->
    case yz_index:exists(Name) of
        true -> ok = yz_index:local_remove(Name);
        false -> ok
    end.

-spec remove_indexes(index_set()) -> ok.
remove_indexes(Names) ->
    [ok = remove_index(N) || N <- Names],
    ok.

-spec remove_node(node(), list()) -> list().
remove_node(Node, Mapping) ->
    proplists:delete(Node, Mapping).

-spec remove_nodes([node()], list()) -> list().
remove_nodes(Nodes, Mapping) ->
    lists:foldl(fun remove_node/2, Mapping, Nodes).

%% @private
%%
%% @doc Remove documents for any data not owned by this node.
-spec remove_non_owned_data() -> ok.
remove_non_owned_data() ->
    Indexes = ordsets:to_list(yz_solr:cores()),
    Removed = [{Index, yz_index:remove_non_owned_data(Index)}
               || Index <- Indexes],
    [maybe_log(R) || R <- Removed],
    ok.

send_ring_event(Ring) ->
    gen_server:cast(?MODULE, {ring_event, Ring}).

set_mapping(Mapping) ->
    true = ets:insert(?YZ_EVENTS_TAB, [{mapping, Mapping}]),
    ok.

set_tick() ->
    Interval = get_tick_interval(),
    erlang:send_after(Interval, ?MODULE, tick),
    ok.

sync_data(Removed, Added) ->
    [sync_added(Bucket) || Bucket <- Added],
    [sync_removed(Bucket) || Bucket <- Removed],
    ok.

sync_added(Bucket) ->
    Params = [{q, <<"_yz_rb:",Bucket/binary>>},{wt,<<"json">>}],
    case yz_solr:search(?YZ_DEFAULT_INDEX, [], Params) of
        {_, Resp} ->
            Struct = mochijson2:decode(Resp),
            NumFound = yz_solr:get_path(Struct, [<<"response">>, <<"numFound">>]),
            if NumFound > 0 ->
                    lager:info("index flag enabled for bucket ~s with existing data", [Bucket]),
                    Qry = mochijson:encode({struct, [{delete, {struct, [{'query', list_to_binary("_yz_rb:" ++ Bucket)}]}}, {commit, {struct, []}}]}),
                    ok = yz_solr:delete_by_query(?YZ_DEFAULT_INDEX, Qry),
                    yz_entropy_mgr:clear_trees();
               true ->
                    ok
            end;
        _ ->
            ok
    end.

sync_removed(Bucket) ->
    lager:info("index flag disabled for bucket ~s", [Bucket]),
    Qry = mochijson:encode({struct, [{delete, {struct, [{'query', <<"*:*">>}]}}, {commit, {struct, []}}]}),
    ok = yz_solr:delete_by_query(binary_to_list(Bucket), Qry),
    yz_entropy_mgr:clear_trees(),
    ok.

sync_indexes(Ring, Removed, Added, Same) ->
    ok = remove_indexes(Removed),
    ok = add_indexes(Ring, Added ++ Same).

watch_ring_events() ->
    riak_core_ring_events:add_sup_callback(fun send_ring_event/1).
