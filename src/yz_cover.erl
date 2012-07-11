-module(yz_cover).
-compile(export_all).
-include("yokozuna.hrl").

%% @doc This module contains functionality related to creating
%%      coverage information for distributed search queries.

%%%===================================================================
%%% API
%%%===================================================================

plan(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    BProps = riak_core_bucket:get_bucket(Index, Ring),
    Selector = all,
    NVal = riak_core_bucket:n_val(BProps),
    NumPrimaries = 1,
    ReqId = erlang:phash2(erlang:now()),
    Service = yokozuna,

    {CoveringSet, _} = riak_core_coverage_plan:create_plan(Selector,
                                                           NVal,
                                                           NumPrimaries,
                                                           ReqId,
                                                           Service),
    {Partitions, Nodes} = lists:unzip(CoveringSet),
    UniqNodes = lists:usort(Nodes),
    FilterPairs = filter_pairs(Ring, NVal, Partitions),
    {UniqNodes, reify(Ring, FilterPairs)}.

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Create a `{LogicalPartition, Include}' filter pair for a given
%%      `{LogicalPartition, Dist}' pair.  `Include' indicates which
%%      replicas should be included for the paired `LogicalPartition'.
%%      The value `all' means all replicas.  If the value if a list of
%%      `lp()' then a replica must has one of the LPs as it's first
%%      primary partition on the preflist.
-spec filter_pair(n(), q(), {lp(), dist()}) -> {lp(), all | [lp()]}.
filter_pair(N, _Q, {LP, N}) ->
    {LP, all};
filter_pair(N, Q, {LP, Dist}) ->
    LPSeq = lists:reverse(lp_seq(N, Q, LP)),
    Filter = lists:sublist(LPSeq, Dist),
    {LP, Filter}.

filter_pairs(Ring, N, CPartitions) ->
    Q = riak_core_ring:num_partitions(Ring),
    LPI = logical_partitions(Ring),
    Logical = make_logical(LPI, CPartitions),
    Pairs = make_pairs(Logical, hd(Logical), []),
    Dist = make_distance_pairs(Q, Pairs),
    make_filter_pairs(N, Q, Dist).

%% @doc Get the distance between the logical partition `LPB' and
%%      `LPA'.
-spec get_distance(q(), lp(), lp()) -> dist().
get_distance(Q, LPA, LPB) when LPB < LPA ->
    %% Wrap around
    BottomDiff = LPB - 1,
    TopDiff = Q - LPA,
    BottomDiff + TopDiff + 1;
get_distance(_Q, LPA, LPB) ->
    LPB - LPA.

%% @doc Map `Partition' to it's logical partition.
-spec logical_partition(logical_idx(), p()) -> lp().
logical_partition(LogicalIndex, Partition) ->
    {Logical, _} = lists:keyfind(Partition, 2, LogicalIndex),
    Logical.

%% @doc Create a mapping from logical to actual partition.
-spec logical_partitions(riak_core_ring:ring()) -> logical_idx().
logical_partitions(Ring) ->
    {Partitions, _} = lists:unzip(riak_core_ring:all_owners(Ring)),
    Q = riak_core_ring:num_partitions(Ring),
    Logical = lists:seq(1, Q),
    lists:zip(Logical, lists:sort(Partitions)).

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
-spec make_distance_pairs(q(), [{lp(), lp()}]) -> [{lp(), dist()}].
make_distance_pairs(Q, PartitionPairs) ->
    [{LPB, get_distance(Q, LPA, LPB)} || {LPA, LPB} <- PartitionPairs].

make_filter_pairs(N, Q, Dist) ->
    [filter_pair(N, Q, DP) || DP <- Dist].

%% @doc Take a list of `Partitions' and create a list of logical
%%      partitions.
-spec make_logical(logical_idx(), [p()]) -> [lp()].
make_logical(LogicalIndex, Partitions) ->
    [logical_partition(LogicalIndex, P) || P <- Partitions].

%% @doc Take a list of logical partitions and pair them up.  Since the
%%      ring wraps the last and first logical partitions must also be
%%      paired.
-spec make_pairs([lp()], lp(), [{lp(), lp()}]) -> [{lp(), lp()}].
make_pairs([Last], First, Pairs) ->
    [{Last, First}|lists:reverse(Pairs)];
make_pairs([A,B|T], _First, Pairs) ->
    make_pairs([B|T], _First, [{A,B}|Pairs]).

%% @doc Map `LP' to actual partition.
-spec partition(logical_idx(), lp()) -> p().
partition(LogicalIndex, LP) ->
    {_, P} = lists:keyfind(LP, 1, LogicalIndex),
    P.

reify(Ring, FilterPairs) ->
    LPI = logical_partitions(Ring),
    [reify_pair(LPI, FilterPair) || FilterPair <- FilterPairs].

%% @doc Take a logical filter and make it concrete.
reify_pair(LPI, {LP, all}) ->
    {partition(LPI, LP), all};
reify_pair(LPI, {LP, IncludeLPs}) ->
    {partition(LPI, LP), [partition(LPI, LP2) || LP2 <- IncludeLPs]}.
