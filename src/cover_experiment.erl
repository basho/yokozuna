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
    %% LI = logical_index(Ring),
    RingSize = ring_size(Ring),
    StartLPs = lists:seq(1, NVal),
    [cover_preflists(RingSize, NVal, [LP]) || LP <- StartLPs].

%% @doc Determine the set of preflists (represented by their first
%%      partition), starting at the initial `Last' value, which cover
%%      the ring space based on the `NVal'.
-spec cover_preflists(ring_size(), n_val(), lp()) -> [lp()].
cover_preflists(RingSize, NVal, Preflists=[Last|_]) ->
    LP = Last + NVal,
    if LP > RingSize ->
            Preflists;
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
    [element(2, lists:keyfind(Node, 1, NodeNumMap)) || Node <- Nodes].
    

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
%%% Misc
%%%===================================================================

%% @doc Print base-2 representation of `Bits'.
print_bits(Bits, NumBuckets) ->    
    io:format("~*.2.0B~n", [NumBuckets, Bits]).

%% @private
-spec ring_size(ring()) -> ring_size().
ring_size(Ring) ->
    riak_core_ring:num_partitions(Ring).

%% testing:
%%
%% Ring = riak_core_ring:fresh(64, foobar).
%% TargetPreflists = cover_experiment:target_preflists(Ring, 3).
%% TV1 = cover_experiment:convert_preflists_to_bit_vector(hd(TargetPreflists)).
%% cover_experiment:print_bits(TV1, 64).
