-module(cover_experiment).
-compile(export_all).
-include("yokozuna.hrl").

-type n_val() :: pos_integer().
-type ring_size() :: pos_integer().

%%%===================================================================
%%% Preflists
%%%===================================================================

%% @doc Given a `Ring' and `NVal' return list of target preflists
%%      which may be used to cover the keyspace.
-spec target_preflists(ring(), n_val()) -> [[lp()]].
target_preflists(Ring, NVal) ->
    RingSize = ring_size(Ring),
    StartLPs = lists:seq(1, NVal),
    [cover_preflists(RingSize, NVal, [LP]) || LP <- StartLPs].

%% Ring = riak_core_ring:fresh(64, foobar).
%% TargetPreflists = cover_experiment:target_preflists(Ring, 3).
%% TV1 = cover_experiment:convert_preflists_to_bit_vector(hd(TargetPreflists)).
%% cover_experiment:print_bits(TV1, 64).

%% @doc Determine the set of preflists (represented by their first
%%      partition), starting at the initial `Last' value, which cover
%%      the ring space based on the `NVal'.
-spec cover_preflists(ring_size(), n_val(), lp()) -> [lp()].
cover_preflists(RingSize, NVal, Preflists=[Last|_]) ->
    LP = Last + NVal,
    if LP >= RingSize ->
            %% If >= then you have wrapped, just choose last preflist
            %% because you know it will cover up to the first
            %% partition.
            [RingSize|Preflists];
       true ->
            cover_preflists(RingSize, NVal, [LP|Preflists])
    end.

%% @doc Convert a list made by `cover_preflists' into a bitmask
%%      represented by number.
-spec convert_preflists_to_bit_vector([lp()]) -> integer().
convert_preflists_to_bit_vector(Preflists) ->
    convert_preflists_to_bit_vector(Preflists, 0).

convert_preflists_to_bit_vector([LP|Preflists], Bits) ->
    LPBits = trunc(math:pow(2, LP)),
    NewBits = Bits bor LPBits,
    convert_preflists_to_bit_vector(Preflists, NewBits);
convert_preflists_to_bit_vector([], Bits) ->
    %% LP numbers start at 1, need to shift to map 1 to 0
    Bits bsr 1.

%%%===================================================================
%%% Preflist-Node Vectors
%%%===================================================================

-type bits() :: integer().
-type p_node_vec_map() :: [{lp(), bits()}].

%% @doc Create a map from partition to node vector.  A node vector is
%%      a set of nodes that a given preflist, starting a the
%%      associated partition, represented as a bit-mask.
-spec partition_node_vector_map([lp()], ring(), n_val(), node_num_map()) ->
                                       p_node_vec_map().
partition_node_vector_map(TargetPreflists, Ring, NVal, NodeNumMap) ->
    LPToOwner = lp_to_owner(Ring),
    RingSize = ring_size(Ring),
    [{LP, node_vector(preflist_nodes(LP, LPToOwner, RingSize, NVal), NodeNumMap)}
     || LP <- TargetPreflists].

-type node_num_map() :: [{node(), pos_integer()}].
-spec node_num_map(ring()) -> node_num_map().
node_num_map(Ring) ->
    Nodes = lists:sort(riak_core_ring:all_members(Ring)),
    NumNodes = length(Nodes),
    Nums = lists:seq(1, NumNodes),
    lists:zip(Nodes, Nums).

-type lp_owner_map() :: [{lp(), node()}].
-spec lp_to_owner(ring()) -> lp_owner_map().
lp_to_owner(Ring) ->
    Owners = riak_core_ring:all_owners(Ring),
    LI = yz_cover:logical_index(Ring),
    [{yz_cover:logical_partition(LI, P), Owner} || {P, Owner} <- Owners].

-spec preflist_nodes(lp(), lp_owner_map(), ring_size(), n_val()) -> [node()].
preflist_nodes(LPStart, LPToOwner, RingSize, NVal) ->
    LPs = lists:seq(LPStart, (LPStart + NVal) - 1),
    LPs2 = [if LP > RingSize -> LP rem RingSize;
               true -> LP
            end || LP <- LPs],
    [element(2, lists:keyfind(LP, 1, LPToOwner)) || LP <- LPs2].

-spec node_vector([node()], node_num_map()) -> bits().
node_vector(Nodes, NodeNumMap) ->
    node_vec([element(2, lists:keyfind(Node, 1, NodeNumMap)) || Node <- Nodes]).

%% @doc Same as convert_preflists_to_bit_vector.
-spec node_vec([node()]) -> bits().
node_vec(Nodes) ->
    node_vec(Nodes, 0).

node_vec([Node|Nodes], Bits) ->
    NodeBits = trunc(math:pow(2, Node)),
    NewBits = Bits bor NodeBits,
    node_vec(Nodes, NewBits);
node_vec([], Bits) ->
    %% Node numbers start at 1, need to shift to map 1 to 0
    Bits bsr 1.

%% Ring2 = riak_core_ring:add_member(foobar, Ring, foobar2).
%% Ring3 = riak_core_ring:add_member(foobar, Ring2, foobar3).
%% NodeNumMap = cover_experiment:node_num_map(Ring3).
%% TP1 = hd(TargetPreflists).
%% cover_experiment:partition_node_vector_map(TP1, Ring3, 3, NodeNumMap).

%%%===================================================================
%%% Node To Partition-Vector Map
%%%===================================================================

-type node_p_vec_map() :: [{node(), bits()}].
-spec node_p_vec_map(lp_owner_map()) -> node_p_vec_map().
node_p_vec_map(LPToOwner) ->
    Grouped = group_by_node(LPToOwner),
    [{Node, p_vec(LPs)} || {Node, LPs} <- Grouped].

%% PVec is vector for all partitions turned on for a node.
-spec p_vec_cover(bits(), ring_size(), n_val()) -> bits().
p_vec_cover(PVec, RingSize, NVal) ->
    PositionsOn = positions_on(PVec, RingSize),
    io:format("PositionsOn: ~p~n", [PositionsOn]),
    %% NVal - 1 because already have 1 bit on
    CoverBits = [cover_bits(Bits, RingSize, NVal - 1) || Bits <- PositionsOn],
    bit_or(CoverBits, 0).

cover_bits(Bits, _RingSize, 0) ->
    Bits;
cover_bits(Bits, RingSize, NVal) ->
    io:format("cover_bits ~p ~p~n", [Bits, NVal]),
    if Bits == 1 ->
            %% At right-most bit, turn on NVal left-most bits, I - 1
            %% because mapping 1-indexed to 0-indexed
            %% 
            %% NVal - 1 because seq is inclusive in range
            %%
            %% TODO: this can be converted to function head
            bit_or([trunc(math:pow(2, I - 1))
                    || I <- lists:seq(RingSize - (NVal - 1), RingSize)], Bits);
       true ->
            cover_bits(Bits bor (Bits div 2), RingSize, NVal - 1)
    end.

bit_or([Bits|Rest], Acc) ->
    bit_or(Rest, Bits bor Acc);
bit_or([], Acc) ->
    Acc.

-spec positions_on(bits(), ring_size()) -> [bits()].
positions_on(Bits, Buckets) ->
    positions_on(Bits, Buckets, []).

positions_on(_Bits, 0, Match) ->
    Match;
positions_on(Bits, Bucket, Match) ->
    BucketVec = trunc(math:pow(2, Bucket - 1)),
    if (BucketVec band Bits) > 0 ->
            positions_on(Bits, Bucket - 1, [BucketVec|Match]);
       true ->
            positions_on(Bits, Bucket - 1, Match)
    end.
    
%% PBIts is vector for given PNum.
%%
%% Which partitions are turned on for a given `PVec'.
%% which_partitions([{PNum, PBits}|PBitList], PVec, Match) ->
    

%% @doc Same as convert_preflists_to_bit_vector.
-spec p_vec([lp()]) -> bits().
p_vec(Preflists) ->
    p_vec(Preflists, 0).

p_vec([LP|Preflists], Bits) ->
    LPBits = trunc(math:pow(2, LP)),
    NewBits = Bits bor LPBits,
    p_vec(Preflists, NewBits);
p_vec([], Bits) ->
    %% LP numbers start at 1, need to shift to map 1 to 0
    Bits bsr 1.

-spec group_by_node(lp_owner_map()) -> [{node(), [lp()]}].
group_by_node(LPToOwner) ->
    F = fun({LP, Node}) -> {Node, LP} end,
    yz_misc:group_by(LPToOwner, F).

%% LPToOwner = cover_experiment:lp_to_owner(Ring).
%% Grouped = cover_experiment:group_by_node(LPToOwner).
%% PVec = cover_experiment:p_vec(element(2,hd(Grouped))).
%% cover_experiment:print_bits(PVec, 64).
%%
%% cover_experiment:node_p_vec_map(LPToOwner).

%%%===================================================================
%%% Target Preflists To Node Ratio
%%%
%%% For each target preflists calculate the ratio owned by each node.
%%%===================================================================

%% @doc For each node in `NodePVecMap' calculate number of partitions
%%      it owners in `TargetPreflists'.  Return the list sorted by
%%      count descending.
-spec node_ratios([lp()], node_p_vec_map()) -> [{node(), integer(), integer()}].
node_ratios(TargetPreflist, NodePVecMap) ->
    PreflistsVector = convert_preflists_to_bit_vector(TargetPreflist),
    Total = count_ones(PreflistsVector),
    Ratios = [{Node, count_ones(PreflistsVector band PVec), Total}
              || {Node, PVec} <- NodePVecMap],
    lists:keysort(2, Ratios).

%% NodePVecMap = cover_experiment:node_p_vec_map(LPToOwner).
%% cover_experiment:node_ratios(TP1, NodePVecMap).

%%%===================================================================
%%% Determine Min Coverage
%%%===================================================================

-spec does_one_node_cover(p_node_vec_map(), node_num_map()) -> [node()].
does_one_node_cover([{_,NodeVec}|PNodeVecMap], NodeNumMap) ->
    does_one_node_cover(PNodeVecMap, NodeVec, NodeNumMap).

does_one_node_cover([{_,NodeVec2}|PNodeVecMap], AccVec, NodeNumMap) ->
    does_one_node_cover(PNodeVecMap, AccVec band NodeVec2, NodeNumMap);
does_one_node_cover([], AccVec, NodeNumMap) ->
    if AccVec > 0 -> which_nodes(NodeNumMap, AccVec, []);
       true -> []
    end.

%% PNodeVecMap = cover_experiment:partition_node_vector_map(TP1, Ring3, 3, NodeNumMap).
%% cover_experiment:does_one_node_cover(PNodeVecMap, NodeNumMap).

which_nodes([{Node,Num}|NodeNumMap], AccVec, Match) ->
    %% Need to subtract by 1 because node numbers are 1-based
    Check = trunc(math:pow(2, Num - 1)) band AccVec,
    if Check > 0 ->
            which_nodes(NodeNumMap, AccVec, [Node|Match]);
       true ->
            which_nodes(NodeNumMap, AccVec, Match)
    end;
which_nodes([], _, Match) ->
    Match.

%%%===================================================================
%%% Misc
%%%===================================================================

%% @doc Count the number of bits turned on.
-spec count_ones(bits()) -> integer().
count_ones(Bits) ->
    count_ones(Bits, 0).

count_ones(0, Count) ->
    Count;
count_ones(Bits, Count) ->
    count_ones(Bits band (Bits - 1), Count + 1).

%% @doc Print base-2 representation of `Bits'.
print_bits(Bits, NumBuckets) ->    
    io:format("~*.2.0B~n", [NumBuckets, Bits]).

%% @private
-spec ring_size(ring()) -> ring_size().
ring_size(Ring) ->
    riak_core_ring:num_partitions(Ring).

%%%===================================================================
%%% Simulation
%%%===================================================================


%% Relies on riak_core_claim_sim which relies on actuall running Riak
%% instance.
simulate(RingSize, NumNodes, NVal) ->
    [Node1|Nodes] = [gen_node(I) || I <- lists:seq(1,NumNodes)],
    Ring1 = riak_core_ring:fresh(RingSize, Node1),
    Cmds = [{join, Node} || Node <- Nodes],
    Opts = [{ring, Ring1}, {cmds, Cmds}, {return_ring, true}],
    Ring2 = riak_core_claim_sim:run(Opts),
    LPToOwner = lp_to_owner(Ring2),
    io:format("LPToOwner: ~p~n", [LPToOwner]),
    NodePVecMap = node_p_vec_map(LPToOwner),
    io:format("NodePVecMap: ~p~n", [NodePVecMap]),
    TPs = target_preflists(Ring2, NVal),
    io:format("TPs: ~p~n", [TPs]),
    NodeNumMap = node_num_map(Ring2),
    io:format("NodeNumMap: ~p~n", [NodeNumMap]),
    TP1 = lists:nth(1, TPs),
    io:format("TP1: ~p~n", [TP1]),
    PNodeVecMap1 = partition_node_vector_map(TP1, Ring2, NVal, NodeNumMap),
    io:format("PNodeVecMap1: ~p~n", [PNodeVecMap1]),
    Ratio1 = node_ratios(TP1, NodePVecMap),
    io:format("Ratio1: ~p~n", [Ratio1]),
    NodePVecMapCover = [{Node, p_vec_cover(PVec, RingSize, NVal)}
                        || {Node, PVec} <- NodePVecMap],
    CorrectRatio1 = node_ratios(TP1, NodePVecMapCover),
    io:format("CorrectRatio1: ~p~n", [CorrectRatio1]),
    Ring2.

gen_node(I) ->
    list_to_atom(lists:flatten(io_lib:format("node~p", [I]))).
