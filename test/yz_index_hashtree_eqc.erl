%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%-------------------------------------------------------------------

%% @doc
%% This module is a Quickcheck eqc_statem test that checks to make sure
%% that the fundamental concepts of Yokozuna's AAE process implementation
%% (i.e. start, insert, update, compare) operate correctly when inserting
%% objects into both a single yokozuna hashtree process and a riak-kv hashtree
%% process where n=1.

%% Based on the differing objects between the trees, the riak-kv hashtree
%% is the canonical source of the data and the count to repair is determined
%% by objects that a yz_index_hashtree is missing.

%% The goal of this test to start modelling Yokozuna's AAE implementation and
%% to make sure it handles tree comparisons correctly.

%% Key Things to Note:
%% There's 1 riak_kv_idx_hashtree && 1 yz_kv_idx_hashtree per vnode.

%% Each idx_hashtree can contain a collection of hashtrees...
%% # of hashtrees per vnode == # in responsible_preflists

%% This currently assumes N=1.

%% We're sidestepping Yokozuna's logical partitioning
%% (http://docs.basho.com/riak/latest/dev/advanced/search/#Indexes) for testing
%% purposes.

-module(yz_index_hashtree_eqc).
-compile(export_all).

-include("yokozuna.hrl").

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(RING_SIZE, 16).
-define(TEST_INDEX_N, {0, 1}).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

insert_method() ->
    oneof([sync, async]).

index_hashtree_test_() ->
    {timeout, 60,
     fun() ->
         ?assert(eqc:quickcheck(?QC_OUT(eqc:testing_time(29, prop_correct()))))
     end
    }.

%% We want to constrain all data to fit in the last portion of our
%% hash space so that it maps to partition 0.
hashfun({_Bucket, _Key}=ID) ->
    %% Top valid hash value
    Top = (1 bsl 160) - 1,
    %% Calculate partition size
    PartitionSize = chash:ring_increment(?RING_SIZE),
    %% Generate an integer hash
    Hash = intify_sha(riak_core_util:chash_std_keyfun(ID)),
    %% Map the hash to 1/?RING_SIZE of the full hash space
    SmallHash = Hash rem PartitionSize,
    %% Force the hash into the last 1/?RING_SIZE block
    Top - SmallHash.

intify_sha(<<Int:160>>) ->
    Int.

setup() ->
    eqc_util:set_core_envs([{chash_keyfun, {?MODULE, hashfun}}]),
    eqc_util:set_kv_envs(),
    eqc_util:set_yokozuna_envs(),
    eqc_util:start_mock_components([{ring_size, ?RING_SIZE}]),
    ok.

cleanup() ->
    eqc_util:cleanup_mock_components(),
    ok.

test() ->
    test(100).

check() ->
    eqc:check(prop_correct()).

check(F) ->
    {ok, B} = file:read_file(F),
    CE = binary_to_term(B),
    eqc:check(eqc_statem:show_states(prop_correct()), CE).

test(N) ->
    eqc:quickcheck(numtests(N, prop_correct())).

%% Use General State Record
%% TODO: Move to sets/ordsets as this expands
-record(state, {yz_idx_tree,
                kv_idx_tree,
                yz_idx_objects = dict:new(),
                kv_idx_objects = dict:new(),
                trees_updated = false,
                both = []}).

%% Initialize State
initial_state() ->
    #state{}.

%% ------ Grouped operator: start_yz_tree
%% @doc start_yz_tree_command - Command generator
-spec start_yz_tree_command(S :: eqc_statem:symbolic_state()) ->
        eqc_gen:gen(eqc_statem:call()).
start_yz_tree_command(_S) ->
    {call, ?MODULE, start_yz_tree, []}.

%% @doc start_yz_tree_pre - Precondition for generation
-spec start_yz_tree_pre(S :: eqc_statem:symbolic_state()) -> boolean().
start_yz_tree_pre(S) ->
    S#state.yz_idx_tree == undefined.

%% @doc start_yz_tree_next - Next state function
-spec start_yz_tree_next(S :: eqc_statem:symbolic_state(),
                         V :: eqc_statem:var(),
                         Args :: [term()]) -> eqc_statem:symbolic_state().
start_yz_tree_next(S, V, _Args) ->
    S#state{yz_idx_tree=V, yz_idx_objects=dict:store(?TEST_INDEX_N, dict:new(),
                                               S#state.yz_idx_objects)}.

%% @doc start_yz_tree_post - Postcondition for start_yz_tree
-spec start_yz_tree_post(S :: exqc_statem:dynamic_state(),
                         Args :: [term()], R :: {ok, tree()}) -> true | term().
start_yz_tree_post(_S, _Args, R) ->
    case R of
        {ok, TreePid} ->
            is_process_alive(TreePid);
        _ ->
            error
    end.

%% @doc Spawns a yz idx hashtree gen_server process with a default `Index`
%%      and a list of default `Responsible Preflists`.
%%      NOTE: Currently defaults to N=1 and a single RP
-spec start_yz_tree() -> {ok, tree()}.
start_yz_tree() ->
    yz_index_hashtree:start_link(0, [?TEST_INDEX_N]).

%% ------ Grouped operator: start_kv_tree
%% @doc start_kv_tree_command - Command generator
-spec start_kv_tree_command(S :: eqc_statem:symbolic_state()) ->
        eqc_gen:gen(eqc_statem:call()).
start_kv_tree_command(_S) ->
    {call, ?MODULE, start_kv_tree, []}.

%% @doc start_kv_tree_pre - Precondition for generation
-spec start_kv_tree_pre(S :: eqc_statem:symbolic_state()) -> boolean().
start_kv_tree_pre(S) ->
    S#state.kv_idx_tree == undefined.

%% @doc start_kv_tree_next - Next state function
-spec start_kv_tree_next(S :: eqc_statem:symbolic_state(),
                         V :: eqc_statem:var(),
                         Args :: [term()]) -> eqc_statem:symbolic_state().
start_kv_tree_next(S, V, _Args) ->
    S#state{kv_idx_tree=V, kv_idx_objects=dict:store(?TEST_INDEX_N, dict:new(),
                                                     S#state.kv_idx_objects)}.

%% @doc start_kv_tree_post - Postcondition for start_kv_tree
-spec start_kv_tree_post(S :: eqc_statem:dynamic_state(),
                         Args :: [term()], R :: {ok, tree()}) -> true | term().
start_kv_tree_post(_S, _Args, R) ->
    case R of
        {ok, TreePid} ->
            is_process_alive(TreePid);
        _ ->
            error
    end.

%% @doc Spawns a kv idx hashtree gen_server process with a default `Index`
-spec start_kv_tree() -> {ok, tree()}.
start_kv_tree() ->
    riak_kv_index_hashtree:start_link(0, ?MODULE, []).

%% ------ Grouped operator: insert_yz_tree
%% @doc insert_yz_tree_command - Command generator
-spec insert_yz_tree_command(S :: eqc_statem:symbolic_state()) ->
        eqc_gen:gen(eqc_statem:call()).
insert_yz_tree_command(S) ->
    {call, ?MODULE, insert_yz_tree, [insert_method(), eqc_util:riak_object(),
                                     S#state.yz_idx_tree]}.

%% @doc insert_yz_tree_pre - Precondition for insert_yz_tree
-spec insert_yz_tree_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
insert_yz_tree_pre(S, _Args) ->
    S#state.yz_idx_tree /= undefined.

%% @doc insert_yz_tree_next - Next state function
-spec insert_yz_tree_next(S :: eqc_statem:symbolic_state(),
                          V :: eqc_statem:var(),
                          Args :: [term()]) -> eqc_statem:symbolic_state().
insert_yz_tree_next(S, _V, [_, RObj, _]) ->
    {ok, TreeData} = dict:find(?TEST_INDEX_N, S#state.yz_idx_objects),
    S#state{yz_idx_objects=dict:store(?TEST_INDEX_N,
                                      set_treedata(RObj, TreeData),
                                      S#state.yz_idx_objects),
            trees_updated=false}.

%% @doc insert_yz_tree_post - Postcondition for insert_yz_tree
-spec insert_yz_tree_post(S :: eqc_statem:dynamic_state(),
                          Args :: [term()], R :: ok) -> true | term().
insert_yz_tree_post(_S, _Args, _Res) ->
    true.

-spec insert_yz_tree(sync|async, obj(), {ok, tree()}) -> ok.
insert_yz_tree(Method, RObj, {ok, TreePid}) ->
    BKey = eqc_util:get_bkey_from_object(RObj),
    yz_index_hashtree:insert(Method, ?TEST_INDEX_N, BKey,
                             yz_kv:hash_object(RObj), TreePid, []).

%% %% ------ Grouped operator: insert_kv_tree
%% @doc insert_kv_tree_command - Command generator
-spec insert_kv_tree_command(S :: eqc_statem:symbolic_state()) ->
        eqc_gen:gen(eqc_statem:call()).
insert_kv_tree_command(S) ->
    {call, ?MODULE, insert_kv_tree, [insert_method(), eqc_util:riak_object(),
                                     S#state.kv_idx_tree]}.

%% @doc insert_kv_tree_pre - Precondition for insert_kv_tree
-spec insert_kv_tree_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
insert_kv_tree_pre(S, _Args) ->
    S#state.kv_idx_tree /= undefined.

%% @doc insert_kv_tree_next - Next state function
-spec insert_kv_tree_next(S :: eqc_statem:symbolic_state(),
                          V :: eqc_statem:var(),
                          Args :: [term()]) -> eqc_statem:symbolic_state().
insert_kv_tree_next(S, _V, [_, RObj, _]) ->
    {ok, TreeData} = dict:find(?TEST_INDEX_N, S#state.kv_idx_objects),
    S#state{kv_idx_objects=dict:store(?TEST_INDEX_N,
                                      set_treedata(RObj, TreeData),
                                      S#state.kv_idx_objects),
            trees_updated=false}.

%% @doc insert_kv_tree_post - Postcondition for insert_kv_tree
-spec insert_kv_tree_post(S :: eqc_statem:dynamic_state(),
                          Args :: [term()], R :: term()) -> true | term().
insert_kv_tree_post(_S, _Args, _Res) ->
    true.

-spec insert_kv_tree(sync|async, obj(), {ok, tree()}) -> ok.
insert_kv_tree(Method, RObj, {ok, TreePid}) ->
    {Bucket, Key} = eqc_util:get_bkey_from_object(RObj),
    Items = [{object, {Bucket, Key}, RObj}],
    case Method of
        sync ->
            riak_kv_index_hashtree:insert(Items, [], TreePid);
        async ->
            riak_kv_index_hashtree:async_insert(Items, [], TreePid)
    end.

%% %% ------ Grouped operator: insert_both
%% %% @doc insert_both_command - Command generator
-spec insert_both_command(S :: eqc_statem:symbolic_state()) ->
        eqc_gen:gen(eqc_statem:call()).
insert_both_command(S) ->
    {call, ?MODULE, insert_both, [insert_method(), eqc_util:riak_object(),
                                  S#state.yz_idx_tree, S#state.kv_idx_tree]}.

%% @doc insert_both_pre - Precondition for insert_both
-spec insert_both_pre(S :: eqc_statem:symbolic_state(),
                      Args :: [term()]) -> boolean().
insert_both_pre(S, _Args) ->
    S#state.yz_idx_tree /= undefined andalso S#state.kv_idx_tree /= undefined.

%% @doc insert_both_next - Next state function
-spec insert_both_next(S :: eqc_statem:symbolic_state(),
                       V :: eqc_statem:var(),
                       Args :: [term()]) -> eqc_statem:symbolic_state().
insert_both_next(S, _V, [_, RObj, _, _]) ->
    {ok, YZTreeData} = dict:find(?TEST_INDEX_N, S#state.yz_idx_objects),
    {ok, KVTreeData} = dict:find(?TEST_INDEX_N, S#state.kv_idx_objects),
    S#state{yz_idx_objects=dict:store(?TEST_INDEX_N,
                                      set_treedata(RObj, YZTreeData),
                                      S#state.yz_idx_objects),
            kv_idx_objects=dict:store(?TEST_INDEX_N,
                                      set_treedata(RObj, KVTreeData),
                                      S#state.kv_idx_objects),
            trees_updated=false}.

%% @doc insert_both_post - Postcondition for insert_both
-spec insert_both_post(S :: eqc_statem:dynamic_state(),
                       Args :: [term()], R :: term()) -> true | term().
insert_both_post(_S, _Args, _Res) ->
    true.

-spec insert_both(sync|async, obj(), {ok, tree()}, {ok, tree()}) -> {ok, ok}.
insert_both(Method, RObj, YZOkTree, KVOkTree) ->
    {insert_yz_tree(Method, RObj, YZOkTree),
     insert_kv_tree(Method, RObj, KVOkTree)}.

%% ------ Grouped operator: update
%% @doc update_command - Command generator
-spec update_command(S :: eqc_statem:symbolic_state()) ->
        eqc_gen:gen(eqc_statem:call()).
update_command(S) ->
    {call, ?MODULE, update, [S#state.yz_idx_tree, S#state.kv_idx_tree]}.

%% @doc update_pre - Precondition for update
-spec update_pre(S :: eqc_statem:symbolic_state(),
                 Args :: [term()]) -> boolean().
update_pre(S, _Args) ->
    S#state.yz_idx_tree /= undefined andalso S#state.kv_idx_tree /= undefined.

%% @doc update_next - Next state function
-spec update_next(S :: eqc_statem:symbolic_state(),
                  V :: eqc_statem:var(),
                  Args :: [term()]) -> eqc_statem:symbolic_state().
update_next(S, _Value, _Args) ->
    S#state{trees_updated=true}.

%% @doc update_post - Postcondition for update
-spec update_post(S :: eqc_statem:dynamic_state(),
                  Args :: [term()], R :: term()) -> true | term().
update_post(S, _Args, _Res) ->
    KVTreeData = dict:fetch(?TEST_INDEX_N, S#state.kv_idx_objects),

    ModelKVKeyCount = dict:size(KVTreeData),

    RealKVKeyCount = count_kv_keys(element(2, S#state.kv_idx_tree)),

    YZTreeData = dict:fetch(?TEST_INDEX_N, S#state.yz_idx_objects),

    ModelYZKeyCount = dict:size(YZTreeData),

    RealYZKeyCount = count_yz_keys(element(2, S#state.yz_idx_tree)),

    eq(ModelKVKeyCount, RealKVKeyCount) and
        eq(ModelYZKeyCount, RealYZKeyCount).

%% @doc Update (commit) hashtrees.
-spec update({ok, tree()}, {ok, tree()}) -> ok.
update({ok, YZTreePid}, {ok, KVTreePid}) ->
    yz_index_hashtree:update(?TEST_INDEX_N, YZTreePid),
    riak_kv_index_hashtree:update(?TEST_INDEX_N, KVTreePid),
    ok.

%% ------ Grouped operator: compare
%% @doc compare_command - Commnd generator
-spec compare_command(S :: eqc_statem:symbolic_state()) ->
        eqc_gen:gen(eqc_statem:call()).
compare_command(S) ->
    {call, ?MODULE, compare, [S#state.yz_idx_tree, S#state.kv_idx_tree]}.

%% @doc compare_pre - Precondition for compare
-spec compare_pre(S :: eqc_statem:symbolic_state(),
                   Args :: [term()]) -> boolean().
compare_pre(S, _Args) ->
    S#state.yz_idx_tree /= undefined andalso S#state.kv_idx_tree /= undefined
        andalso S#state.trees_updated.

%% @doc compare_next - Next state function
-spec compare_next(S :: eqc_statem:symbolic_state(),
                    V :: eqc_statem:var(),
                    Args :: [term()]) -> eqc_statem:symbolic_state().
compare_next(S, _V, _Args) ->
    S.

%% @doc compare_post - Postcondition for compare
-spec compare_post(S :: eqc_statem:dynamic_state(),
                    Args :: [term()], R :: term()) -> true | term().
compare_post(S, _Args, Res) ->
    YZTreeData = dict:fetch(?TEST_INDEX_N, S#state.yz_idx_objects),
    KVTreeData = dict:fetch(?TEST_INDEX_N, S#state.kv_idx_objects),

    HashDifferAcc = ordsets:new(),

    LeftDiff = dict:fold(fun(BKey, Hash, {Count, HashDiff}) ->
                                 case dict:find(BKey, KVTreeData) of
                                     {ok, Hash} -> {Count, HashDiff};
                                     {ok, _OtherHash} -> {Count, ordsets:add_element(BKey, HashDiff)};
                                     error -> {Count+1, HashDiff}
                                 end
                         end, {0, HashDifferAcc}, YZTreeData),
    {C, HD} = dict:fold(fun(BKey, Hash, {Count, HashDiff}) ->
                                 case dict:find(BKey, YZTreeData) of
                                     {ok, Hash} -> {Count, HashDiff};
                                     {ok, _OtherHash} -> {Count, ordsets:add_element(BKey, HashDiff)};
                                     error -> {Count+1, HashDiff}
                                 end
                         end, LeftDiff, KVTreeData),

    RightDiff = C + ordsets:size(HD),
    eq(RightDiff, Res).

%% @doc compare - Runs comparison between *local* YZ Idx Hashtree and
%%      *remote* KV Idx Hashtree.
-spec compare({ok, tree()}, {ok, tree()}) -> integer().
compare({ok, YZTreePid}, {ok, KVTreePid}) ->
    Remote = fun(get_bucket, {L, B}) ->
                     riak_kv_index_hashtree:exchange_bucket(?TEST_INDEX_N, L, B,
                                                            KVTreePid);
                (key_hashes, Segment) ->
                     riak_kv_index_hashtree:exchange_segment(?TEST_INDEX_N,
                                                             Segment,
                                                             KVTreePid);
                (_, _) ->
                     ok
             end,

    AccFun = fun(KeyDiff, Count) ->
                     lists:foldl(fun(Diff, InnerCount) ->
                                         case repair(0, Diff) of
                                             full_repair -> InnerCount + 1;
                                             _ -> InnerCount
                                         end
                                 end, Count, KeyDiff)
             end,

    yz_index_hashtree:compare(?TEST_INDEX_N, Remote, AccFun, 0, YZTreePid).

%% Property Test
prop_correct() ->
    ?SETUP(fun() -> setup(), fun() -> cleanup() end end,
    ?FORALL(Cmds,commands(?MODULE, #state{}),
            aggregate(command_names(Cmds),
                      ?TRAPEXIT(begin
                                    {H, S, Res} = run_commands(?MODULE, Cmds),

                                    catch yz_index_hashtree:destroy(
                                            element(2, S#state.yz_idx_tree)),
                                    catch riak_kv_index_hashtree:destroy(
                                            element(2, S#state.kv_idx_tree)),

                                    pretty_commands(?MODULE,
                                                    Cmds,
                                                    {H, S, Res},
                                                    Res == ok)
                                end)))).

%% Private
-spec set_treedata(obj(), dict()) -> dict().
set_treedata(RObj, D) ->
    BKey = eqc_util:get_bkey_from_object(RObj),
    dict:store(BKey, yz_kv:hash_object(RObj), D).

-spec repair(p(), keydiff()) -> full_repair | tree_repair | failed_repair.
repair(_, {remote_missing, KeyBin}) ->
    BKey = binary_to_term(KeyBin),
    Index = yz_kv:get_index(BKey),
    case yz_kv:should_index(Index) of
        true ->
            full_repair;
        false ->
            tree_repair
    end;
repair(_Partition, {missing, KeyBin}) ->
    BKey = binary_to_term(KeyBin),
    Index = yz_kv:get_index(BKey),
    case yz_kv:should_index(Index) of
        true ->
            full_repair;
        false ->
            tree_repair
    end;
repair(_Partition, {different, KeyBin}) ->
    BKey = binary_to_term(KeyBin),
    Index = yz_kv:get_index(BKey),
    case yz_kv:should_index(Index) of
        true ->
            full_repair;
        false ->
            tree_repair
    end.

%% @doc Count number of keys in the embedded hashtree(s)
count_yz_keys(YZIndex) ->
    lists:sum(lists:map(fun(T) -> count_hashes(T) end, extract_yz_trees(YZIndex))).

extract_yz_trees(YZIndex) ->
    lists:map(fun({_Idx, Tree}) -> Tree end,
              yz_index_hashtree:get_trees({test, YZIndex})).

count_kv_keys(KVIndex) ->
    lists:sum(lists:map(fun(T) -> count_hashes(T) end, extract_kv_trees(KVIndex))).

extract_kv_trees(KVIndex) ->
    lists:map(fun({_Idx, Tree}) -> Tree end,
              riak_kv_index_hashtree:get_trees({test, KVIndex})).

count_hashes(Tree) ->
    NumLevels = hashtree:levels(Tree),
    lists:sum(lists:flatten(chase_level(2, NumLevels, hashtree:get_bucket(1, 0, Tree), Tree))).

chase_level(NextLevel, MaxLevel, Bucket, Tree) when NextLevel > MaxLevel ->
    lists:sum(
      lists:map(fun(Id) -> count_items_in_segments(hashtree:key_hashes(Tree, Id)) end,
                orddict:fetch_keys(Bucket)));
chase_level(NextLevel, MaxLevel, Bucket, Tree) ->
    lists:map(fun(Id) -> chase_level(NextLevel+1, MaxLevel,
                                     hashtree:get_bucket(NextLevel, Id, Tree),
                                     Tree) end,
              orddict:fetch_keys(Bucket)).

count_items_in_segments(Segments) ->
    lists:sum(lists:map(fun({_Segment, Dict}) -> length(orddict:fetch_keys(Dict)) end,
                        Segments)).

-endif.
-endif.
