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
%% Key Things to Note:
%% There's 1 riak_kv_idx_hashtree && 1 yz_kv_idx_hashtree per vnode.

%% Each idx_hashtree can contain a collection of hashtrees...
%% # of hashtrees per vnode == # in responsible_preflists

%% This this current assumes N=1.

%% Much of the functionality happens in the compare grouped operator, as to
%% not have to store actual hashtrees for various generations.


-module(yz_index_hashtree_eqc).
-compile(export_all).

-include("yokozuna.hrl").

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-include_lib("eunit/include/eunit.hrl").

insert_method() ->
    oneof([sync, async]).

index_hashtree_test_() ->
    {setup,
     fun setup/0,
     fun (_) -> cleanup() end,
     {timeout, 60,
      fun() ->
              ?assert(eqc:quickcheck(?QC_OUT(eqc:testing_time(29,
                  prop_correct()))))
      end
     }}.

setup() ->
    eqc_util:set_core_envs(),
    eqc_util:set_kv_envs(),
    eqc_util:set_yokozuna_envs(),
    eqc_util:start_mock_components(),
    ok.

cleanup() ->
    eqc_util:cleanup_mock_components(),
    ok.

test() ->
    test(100).

test(N) ->
    setup(),
    try eqc:quickcheck(numtests(N, prop_correct()))
    after
        cleanup()
    end.

%% Use General State Record
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
    S#state{yz_idx_tree=V, yz_idx_objects=dict:store({0, 1}, dict:new(),
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
    yz_index_hashtree:start_link(0, [{0, 1}]).

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
    S#state{kv_idx_tree=V, kv_idx_objects=dict:store({0, 1}, dict:new(),
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
    {ok, TreeData} = dict:find({0, 1}, S#state.yz_idx_objects),
    S#state{yz_idx_objects=dict:store({0, 1},
                                      set_treedata(RObj, TreeData),
                                      S#state.yz_idx_objects),
            trees_updated=false}.

%% @doc insert_yz_tree_post - Postcondition for inserfsft_yz_tree
-spec insert_yz_tree_post(S :: eqc_statem:dynamic_state(),
                          Args :: [term()], R :: ok) -> true | term().
insert_yz_tree_post(_S, _Args, _Res) ->
    true.

-spec insert_yz_tree(sync|async, obj(), {ok, tree()}) -> ok.
insert_yz_tree(Method, RObj, {ok, TreePid}) ->
    BKey = eqc_util:get_bkey_from_object(RObj),
    yz_index_hashtree:insert(Method, {0, 1}, BKey,
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
    {ok, TreeData} = dict:find({0, 1}, S#state.kv_idx_objects),
    S#state{kv_idx_objects=dict:store({0, 1},
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
    Items = [{void, {Bucket, Key}, RObj}],
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
    {ok, YZTreeData} = dict:find({0, 1}, S#state.yz_idx_objects),
    {ok, KVTreeData} = dict:find({0, 1}, S#state.kv_idx_objects),
    S#state{yz_idx_objects=dict:store({0, 1},
                                      set_treedata(RObj, YZTreeData),
                                      S#state.yz_idx_objects),
            kv_idx_objects=dict:store({0, 1},
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
update_post(_S, _Args, _Res) ->
    true.

%% @doc Update (commit) hashtrees.
-spec update({ok, tree()}, {ok, tree()}) -> ok.
update({ok, YZTreePid}, {ok, KVTreePid}) ->
    yz_index_hashtree:update({0, 1}, YZTreePid),
    riak_kv_index_hashtree:update({0, 1}, KVTreePid),
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
    YZTreeData = dict:fetch({0, 1}, S#state.yz_idx_objects),
    KVTreeData = dict:fetch({0, 1}, S#state.kv_idx_objects),

    FoldFun = fun(D) -> dict:fold(fun(BKey, Hash, Acc) ->
                                         hashtree:insert(
                                           term_to_binary(BKey),
                                           Hash,
                                           Acc)
                                 end, hashtree:new(), D)
              end,

    YZHashTree = hashtree:update_tree(FoldFun(YZTreeData)),

    RemoteKVHashTree = spawn_link(
                         fun() ->
                                 Tree = hashtree:update_tree(FoldFun(KVTreeData)),
                                 message_loop(Tree, 0, 0)
                         end),

    Count = hashtree_compare(YZHashTree, RemoteKVHashTree),

    {YZKeys, KVKeys, ModelDiff, ValueDiff} =
        check_model_against_keys(YZTreeData, KVTreeData, Count),
    if ModelDiff =:= ValueDiff andalso Res =:= Count ->
            true;
       true -> {compare_post, Res, Count, ModelDiff, ValueDiff, YZKeys, KVKeys}
    end.

%% @doc compare - Runs comparison between *local* YZ Idx Hashtree and
%%      *remote* KV Idx Hashtree.
-spec compare({ok, tree()}, {ok, tree()}) -> integer().
compare({ok, YZTreePid}, {ok, KVTreePid}) ->
    Remote = fun(get_bucket, {L, B}) ->
                     riak_kv_index_hashtree:exchange_bucket({0, 1}, L, B,
                                                            KVTreePid);
                (key_hashes, Segment) ->
                     riak_kv_index_hashtree:exchange_segment({0, 1}, Segment,
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

    yz_index_hashtree:compare({0, 1}, Remote, AccFun, 0, YZTreePid).

%% Property Test
prop_correct() ->
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
                                end))).

%% Private
-spec set_treedata(obj(), dict()) -> dict().
set_treedata(RObj, D) ->
    BKey = eqc_util:get_bkey_from_object(RObj),
    dict:store(BKey, yz_kv:hash_object(RObj), D).

-spec hashtree_compare(hashtree(), pid()) -> non_neg_integer().
hashtree_compare(Tree, RemotePid) ->
    Remote = fun(get_bucket, {L, B}) ->
                     RemotePid ! {get_bucket, self(), L, B},
                     receive {remote, X} -> X end;
                (key_hashes, Segment) ->
                     RemotePid ! {key_hashes, self(), Segment},
                     receive {remote, X} -> X end
             end,

    AccFun = fun(Keys, KeyAcc) ->
                     Keys ++ KeyAcc
             end,

    KeyDiff = hashtree:compare(Tree, Remote, AccFun, []),

    RemotePid ! done,

    %% This returns diffs for terms remote_missing and missing (not different)
    %% b/c we always make sure to update hashtrees.
    length(lists:usort([M || {T, M} <- KeyDiff, (T =:= remote_missing)
                                or (T =:= missing)])).

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
repair(Partition, {missing, KeyBin}) ->
    BKey = binary_to_term(KeyBin),
    Index = yz_kv:get_index(BKey),
    case yz_kv:local_get(Partition, BKey) of
        {ok, _} ->
            case yz_kv:should_index(Index) of
                true ->
                    full_repair;
                false ->
                    tree_repair
            end;
        _Other ->
            failed_repair
    end.

-spec message_loop(hashtree(), non_neg_integer(), non_neg_integer()) -> ok.
message_loop(Tree, Msgs, Bytes) ->
    receive
        {get_bucket, From, L, B} ->
            Reply = hashtree:get_bucket(L, B, Tree),
            From ! {remote, Reply},
            Size = byte_size(term_to_binary(Reply)),
            message_loop(Tree, Msgs+1, Bytes+Size);
        {key_hashes, From, Segment} ->
            [{_, KeyHashes2}] = hashtree:key_hashes(Tree, Segment),
            Reply = KeyHashes2,
            From ! {remote, Reply},
            Size = byte_size(term_to_binary(Reply)),
            message_loop(Tree, Msgs+1, Bytes+Size);
        done ->
            ok
    end.

-spec intersection(list(), list()) -> list().
intersection(List1, List2) ->
	[Y || X <- List1, Y <- List2, X==Y].

%% @doc Check model hashtree count against diffcount against bkeys.
-spec check_model_against_keys(dict(), dict(), non_neg_integer()) ->
                                      {list(), list(), non_neg_integer(),
                                       non_neg_integer()}.
check_model_against_keys(YZTreeData, KVTreeData, ModelDiffCount) ->
    YZKeys = dict:fetch_keys(YZTreeData),
    KVKeys = dict:fetch_keys(KVTreeData),

    SharedKeys = intersection(YZKeys, KVKeys),
    DiffCount = length(YZKeys -- SharedKeys) + length(KVKeys -- SharedKeys),
    {YZKeys, KVKeys, ModelDiffCount, DiffCount}.

-endif.
-endif.
