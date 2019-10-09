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
%% @doc This module contains functionality related to creating
%%      coverage information for distributed search queries.

-module(yz_cover).
-compile([export_all, nowarn_export_all]).      % @todo //lelf
-behavior(gen_server).
-export([code_change/3,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         init/1,
         terminate/2]).
-include("yokozuna.hrl").

-record(state, {
          %% The ring used to calculate the current cached plan.
          ring_used :: ring() | undefined
         }).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Retrieve the ring used for the current plan. In rare cases the
%% ring cannot be determined and `unknown' will be returned. It is up
%% to the caller how to interpret this.
-spec get_ring_used() -> ring() | unknown.
get_ring_used() ->
    case riak_core_util:proxy_spawn(fun() -> gen_server:call(?MODULE, get_ring_used, 5000) end) of
	{error, _} ->
	    unknown;
	undefined ->
	    unknown;
	Ring ->
	    Ring
    end.

-spec logical_partitions(ring(), ordset(p())) -> ordset(lp()).
logical_partitions(Ring, Partitions) ->
    LI = logical_index(Ring),
    ordsets:from_list([logical_partition(LI, P) || P <- Partitions]).

%% @doc Get the coverage plan for `Index'.
-spec plan(index_name()) -> {ok, plan()} | {error, term()}.
plan(Index) ->
    NVal = yz_index:get_n_val_from_index(Index),
    case mochiglobal:get(?INT_TO_ATOM(NVal), undefined) of
        undefined -> calc_plan(NVal, yz_misc:get_ring(transformed));
        Plan -> Plan
    end.

-spec reify_partitions(ring(), ordset(lp())) -> ordset(p()).
reify_partitions(Ring, LPartitions) ->
    LI = logical_index(Ring),
    ordsets:from_list([partition(LI, LP) || LP <- LPartitions]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% Callbacks
%%%===================================================================

init([]) ->
    schedule_tick(),
    add_node_watcher_callback(),
    {ok, #state{ring_used=undefined}}.

handle_cast(update_all_plans, S) ->
    Ring = yz_misc:get_ring(transformed),
    ok = update_all_plans(Ring),
    {noreply, S#state{ring_used=Ring}}.

handle_info(tick, S) ->
    Ring = yz_misc:get_ring(transformed),
    ok = update_all_plans(Ring),
    schedule_tick(),
    {noreply, S#state{ring_used=Ring}};

handle_info(Req, S) ->
    lager:warning("Unexpected request ~p", [Req]),
    {noreply, S}.

handle_call(get_ring_used, _, S) ->
    Ring = S#state.ring_used,
    {reply, Ring, S};
handle_call(Req, _, S) ->
    lager:warning("Unexpected request ~p", [Req]),
    {noreply, S}.

code_change(_, S, _) ->
    {ok, S}.

terminate(_, _) ->
    ok.

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Create a covering set using logical partitions and add
%%      filtering information to eliminate overlap.
-spec add_filtering(n(), q(), logical_idx(), p_set()) -> logical_cover_set().
add_filtering(N, Q, LPI, PS) ->
    CS2 = make_logical(LPI, PS),
    CS3 = yz_misc:make_pairs(CS2),
    CS4 = make_distance_pairs(Q, CS3),
    make_cover_set(N, Q, CS4).

%% @private
%%
%% @doc Calculate a plan from the `NVal' and then store an entry
%%      of the NVal's atom in the plan cache.
-spec cache_plan(n(), ring()) -> ok.
cache_plan(NVal, Ring) ->
    case calc_plan(NVal, Ring) of
        {error, _} ->
            mochiglobal:put(?INT_TO_ATOM(NVal), undefined);
        {ok, Plan} ->
            mochiglobal:put(?INT_TO_ATOM(NVal), {ok, Plan})
    end,
    ok.

%% @private
%%
%% @doc Calculate a plan for the `Index'.
-spec calc_plan(n(), ring()) -> {ok, plan()} | {error, term()}.
calc_plan(NVal, Ring) ->
    NumPartitions = riak_core_ring:num_partitions(Ring),
    CoveragePlan = create_coverage_plan(NVal),
    maybe_filter_plan(CoveragePlan, Ring, NVal, NumPartitions).

%% @private
%%
%% @doc Create a Riak core coverage plan.
-spec create_coverage_plan(n()) -> term().
create_coverage_plan(NVal) ->
    ReqId = erlang:phash2(make_ref()),
    NumPrimaries = 1,
    Selector=all,
    riak_core_coverage_plan:create_plan(Selector,
                                        NVal,
                                        NumPrimaries,
                                        ReqId,
                                        ?YZ_SVC_NAME).

%% @doc Get the distance between the logical partition `LPB' and
%%      `LPA'.
-spec get_distance(q(), lp_node(), lp_node()) -> dist().
get_distance(Q, {LPA,_}, {LPB,_}) when LPB < LPA ->
    %% Wrap around
    BottomDiff = LPB - 1,
    TopDiff = Q - LPA,
    BottomDiff + TopDiff + 1;
get_distance(_Q, {LPA,_}, {LPB,_}) ->
    LPB - LPA.

%% @private
%%
-spec get_uniq_nodes(logical_cover_set()) -> [node()].
get_uniq_nodes(CoverSet) ->
    {_Partitions, Nodes} = lists:unzip(CoverSet),
    lists:usort(Nodes).

%% @doc Create a mapping from logical to actual partition.
-spec logical_index(riak_core_ring:riak_core_ring()) -> logical_idx().
logical_index(Ring) ->
    {Partitions, _} = lists:unzip(riak_core_ring:all_owners(Ring)),
    Q = riak_core_ring:num_partitions(Ring),
    Logical = lists:seq(1, Q),
    lists:zip(Logical, lists:sort(Partitions)).

%% @doc Map `Partition' to it's logical partition.
-spec logical_partition(logical_idx(), p()) -> lp().
logical_partition(LogicalIndex, Partition) ->
    {Logical, _} = lists:keyfind(Partition, 2, LogicalIndex),
    Logical.

%% @doc Generate the sequence of `N' partitions leading up to `EndLP'.
%%
%% NOTE: Logical partition numbers start at 1
-spec lp_seq(n(), q(), lp()) -> [lp()].
lp_seq(N, Q, EndLP) ->
    N1 = N - 1,
    StartLP = EndLP - N1,
    if StartLP =< 0 ->
            StartLP2 = Q + StartLP,
            lists:seq(StartLP2, Q) ++ lists:seq(1, EndLP);
       true ->
            lists:seq(StartLP, EndLP)
    end.

%% @doc Take a list of `PartitionPairs' and create a list of
%%      `{LogicalPartition, Distance}' pairs.  The list will contain
%%      the second partition in the original pair and it's distance
%%      from the partition it was paired with.
-spec make_distance_pairs(q(), [{lp_node(), lp_node()}]) ->
                                 [{lp_node(), dist()}].
make_distance_pairs(Q, PartitionPairs) ->
    [{LPB, get_distance(Q, LPA, LPB)} || {LPA, LPB} <- PartitionPairs].


%% @doc Create a `{LogicalPartition, Include}' filter pair for a given
%%      `{LogicalPartition, Dist}' pair.  `Include' indicates which
%%      replicas should be included for the paired `LogicalPartition'.
%%      The value `all' means all replicas.  If the value if a list of
%%      `lp()' then a replica must has one of the LPs as it's first
%%      primary partition on the preflist.
-spec make_cover_pair(n(), q(), {lp_node(), dist()}) -> logical_cover_pair().
make_cover_pair(N, _Q, {LPNode, N}) ->
    {LPNode, all};
make_cover_pair(N, Q, {{LP, Node}, Dist}) ->
    LPSeq = lists:reverse(lp_seq(N, Q, LP)),
    Filter = lists:sublist(LPSeq, Dist),
    {{LP, Node}, Filter}.

-spec make_cover_set(n(), q(), [{lp_node(), dist()}]) -> logical_cover_set().
make_cover_set(N, Q, Cover) ->
    [make_cover_pair(N, Q, DP) || DP <- Cover].

%% @doc Convert the partition set to use logical partitions.
-spec make_logical(logical_idx(), p_set()) -> [lp_node()].
make_logical(LogicalIndex, PSet) ->
    [{logical_partition(LogicalIndex, P), Node} || {P, Node} <- PSet].

%% @private
%%
%% @doc This function converts CovertSet into logical partitions and adds filtering information.
-spec make_logical_and_filter(logical_cover_set(), ring(), n(), pos_integer()) -> logical_cover_set().
make_logical_and_filter(CoverSet, Ring, NVal, NumPartitions) ->
    LPI = logical_index(Ring),
    add_filtering(NVal, NumPartitions, LPI, CoverSet).

%% @private
%%
%% @doc Filter plan or return error.
-spec maybe_filter_plan(term(), ring(), n(), pos_integer()) ->  {ok, plan()} | {error, term()}.
maybe_filter_plan({error, Error}, _, _, _) ->
    {error, Error};
maybe_filter_plan({CoverSet, _}, Ring, NVal, NumPartitions) ->
    LogicalCoverSet = make_logical_and_filter(CoverSet, Ring, NVal, NumPartitions),
    UniqNodes = get_uniq_nodes(CoverSet),
    Mapping = yz_solr:build_mapping(UniqNodes),
    plan_return(length(Mapping) == length(UniqNodes), UniqNodes, LogicalCoverSet, Mapping).

%% @doc Map `LP' to actual partition.
-spec partition(logical_idx(), lp()) -> p().
partition(LogicalIndex, LP) ->
    {_, P} = lists:keyfind(LP, 1, LogicalIndex),
    P.

%% @private
%%
%% @doc Return the plan only if there exists a Solr host-port mapping for each node in the plan.
-spec plan_return(boolean(), [node()], logical_cover_set(), list()) ->  {ok, plan()} | {error, term()}.
plan_return(false, _, _, _) ->
    {error, "Failed to determine Solr port for all nodes in search plan"};
plan_return(true, UniqNodes, LogicalCoverSet, Mapping) ->
    {ok, {UniqNodes, LogicalCoverSet, Mapping}}.

%% @private
%%
%% @doc Schedule next tick to be sent to this server.
-spec schedule_tick() -> ok.
schedule_tick() ->
    erlang:send_after(?YZ_COVER_TICK_INTERVAL, ?MODULE, tick),
    ok.

%% @private
%%
%% @doc Iterate through the list of indexes, calculate a new coverage
%%      plan, and update the cache entry.
-spec update_all_plans(ring()) -> ok.
update_all_plans(Ring) ->
    NVals = get_index_nvals(),
    _ = [ok = cache_plan(N, Ring) || N <- NVals],
    ok.

get_index_nvals() ->
    Indexes = yz_index:get_indexes_from_meta(),
    NVals = lists:usort([yz_index:get_n_val_from_index(I) || I <- Indexes]),
    NVals.

invalidate_plans() ->
    NVals = get_index_nvals(),
    _ = [mochiglobal:put(?INT_TO_ATOM(NVal), undefined) || NVal <- NVals].

add_node_watcher_callback() ->
    riak_core_node_watcher_events:add_guarded_callback(
        fun(Changes) ->
            case lists:member(?YZ_APP_NAME, Changes) of
                true ->
                    invalidate_plans();
                false -> ok
            end
        end).
