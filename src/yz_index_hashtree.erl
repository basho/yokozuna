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

-module(yz_index_hashtree).
-behaviour(gen_server).
-include("yokozuna.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {index,
                built,
                expired :: boolean(),
                lock :: undefined | reference(),
                path,
                build_time,
                trees,
                closed=false}).
-type state() :: #state{}.

-compile(export_all).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawn a hashtree process that manages a hashtree for each
%%      preflist `Index' is responsible for (`RPs').
start(Index, RPs) ->
    supervisor:start_child(yz_index_hashtree_sup, [Index, RPs]).

start_link(Index, RPs) ->
    gen_server:start_link(?MODULE, [Index, RPs], []).

%% @doc Insert the given `Key' and `Hash' pair on `Tree' for the given
%%      `Id'.  The result of a sync call should be ignored since it
%%      uses `catch'.
-spec insert(async | sync, {p(),n()}, bkey(), binary(), tree(), list()) ->
                    ok | term().
insert(async, Id, BKey, Hash, Tree, Options) ->
    gen_server:cast(Tree, {insert, Id, BKey, Hash, Options});

insert(sync, Id, BKey, Hash, Tree, Options) ->
    catch gen_server:call(Tree, {insert, Id, BKey, Hash, Options}, infinity).

%% @doc Delete the `BKey' from `Tree'.  The id will be determined from
%%      `BKey'.  The result of the sync call should be ignored since
%%      it uses catch.
-spec delete(async | sync, {p(),n()}, bkey(), tree()) -> ok.
delete(async, Id, BKey, Tree) ->
    gen_server:cast(Tree, {delete, Id, BKey});

delete(sync, Id, BKey, Tree) ->
    catch gen_server:call(Tree, {delete, Id, BKey}, infinity).

-spec update({p(),n()}, tree()) -> ok.
update(Id, Tree) ->
    gen_server:call(Tree, {update_tree, Id}, infinity).

-spec compare({p(),n()}, hashtree:remote_fun(),
              undefined | hashtree:acc_fun(T), term(), tree()) -> T.
compare(Id, Remote, AccFun, Acc, Tree) ->
    gen_server:call(Tree, {compare, Id, Remote, AccFun, Acc}, infinity).

get_index(Tree) ->
    gen_server:call(Tree, get_index, infinity).

%% @doc Acquire the lock for the specified index_hashtree if not already
%%      locked, and associate the lock with the calling process.
-spec get_lock(tree(), term()) -> ok | not_build | already_locked.
get_lock(Tree, Type) ->
    get_lock(Tree, Type, self()).

%% @doc Acquire the lock for the specified index_hashtree if not already
%%      locked, and associate the lock with the provided pid.
-spec get_lock(tree(), term(), pid()) -> ok | not_build | already_locked.
get_lock(Tree, Type, Pid) ->
    gen_server:call(Tree, {get_lock, Type, Pid}, infinity).

%% @doc Poke the specified `Tree' to ensure the it is built/rebuilt as
%%      needed. This is periodically called by the {@link
%%      yz_entropy_mgr}.
-spec poke(tree()) -> ok.
poke(Tree) ->
    gen_server:cast(Tree, poke).

%% @doc Clear the tree.
-spec clear(tree()) -> ok.
clear(Tree) ->
    gen_server:cast(Tree, clear).

%% @doc Expire the tree.
expire(Tree) ->
    gen_server:call(Tree, expire, infinity).

%% @doc Terminate the `Tree'.
stop(Tree) ->
    gen_server:cast(Tree, stop).

%% @doc Destroy the specified `Tree', which will destroy all
%%      associated hashtrees and terminate.
-spec destroy(tree()) -> ok.
destroy(Tree) ->
    gen_server:call(Tree, destroy, infinity).

%% @doc For testing only, retrieve the hashtree data structures. It is
%% not safe to tamper with these structures due to the LevelDB backend
get_trees({test, Pid}) ->
    gen_server:call(Pid, get_trees, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Index, RPs]) ->
    process_flag(trap_exit, true),
    case determine_data_root() of
        undefined ->
            case riak_kv_entropy_manager:enabled() of
                true ->
                    lager:warning("Neither yokozuna/anti_entropy_data_dir or "
                                  "riak_core/platform_data_dir are defined. "
                                  "Disabling active anti-entropy."),
                    riak_kv_entropy_manager:disable();
                false ->
                    ok
            end,
            ignore;
        Root ->
            Path = filename:join(Root, integer_to_list(Index)),
            S = #state{index=Index,
                       trees=orddict:new(),
                       built=false,
                       expired=false,
                       path=Path},
            S2 = init_trees(RPs, false, S),

            %% If no indexes exist then mark tree as built
            %% immediately. This allows exchange to start immediately
            %% rather than waiting a potentially long time for a build.
            BuiltImmediately = ([] == yz_index:get_indexes_from_meta()),
            _ = case BuiltImmediately of
                    true -> gen_server:cast(self(), build_finished);
                    false -> maybe_expire(S)
                end,
            {ok, S2}
    end.

handle_call({insert, Id, BKey, Hash, Options}, _From, S) ->
    S2 = do_insert(Id, term_to_binary(BKey), Hash, Options, S),
    {reply, ok, S2};

handle_call(get_trees, _From, #state{trees=Trees}=State) ->
    {reply, Trees, State};

handle_call({delete, IdxN, BKey}, _From, S) ->
    S2 = do_delete(IdxN, term_to_binary(BKey), S),
    {reply, ok, S2};

handle_call(get_index, _From, S) ->
    {reply, S#state.index, S};

handle_call({get_lock, Type, Pid}, _From, S) ->
    {Reply, S2} = do_get_lock(Type, Pid, S),
    {reply, Reply, S2};

handle_call({update_tree, Id}, From, S) ->
    lager:debug("Updating tree for partition ~p preflist ~p",
               [S#state.index, Id]),
    apply_tree(Id,
               fun(Tree) ->
                       {SnapTree, Tree2} = hashtree:update_snapshot(Tree),
                       Tree3 = hashtree:set_next_rebuild(Tree2, full),
                       Self = self(),
                       spawn_link(
                         fun() ->
                                 hashtree:update_perform(SnapTree),
                                 gen_server:cast(Self, {updated, Id}),
                                 gen_server:reply(From, ok)
                         end),
                       {noreply, Tree3}
               end,
               S);

handle_call({compare, Id, Remote, AccFun, Acc}, From, S) ->
    do_compare(Id, Remote, AccFun, Acc, From, S),
    {noreply, S};

handle_call(destroy, _From, S) ->
    S2 = destroy_trees(S),
    {stop, normal, ok, S2};

handle_call(expire, _From, S) ->
    S2 = S#state{expired=true},
    {reply, ok, S2};

handle_call(_Request, _From, S) ->
    Reply = ok,
    {reply, Reply, S}.

handle_cast(poke, S) ->
    S2 = do_poke(S),
    {noreply, S2};

handle_cast(build_failed, S) ->
    yz_entropy_mgr:requeue_poke(S#state.index),
    S2 = S#state{built=false},
    {noreply, S2};

handle_cast(build_finished, S) ->
    S2 = do_build_finished(S),
    {noreply, S2};

handle_cast({insert, Id, BKey, Hash, Options}, S) ->
    S2 = do_insert(Id, term_to_binary(BKey), Hash, Options, S),
    {noreply, S2};

handle_cast({delete, IdxN, BKey}, S) ->
    S2 = do_delete(IdxN, term_to_binary(BKey), S),
    {noreply, S2};

handle_cast(clear, S) ->
    S2 = clear_tree(S),
    {noreply, S2};

handle_cast(stop, S) ->
    S2 = close_trees(S),
    {stop, normal, S2};

handle_cast({updated, Id}, State) ->
    Fun = fun(Tree) ->
              {noreply, hashtree:set_next_rebuild(Tree, incremental)}
          end,
    apply_tree(Id, Fun, State);

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({'DOWN', Ref, _, _, _}, S) ->
    S2 = maybe_release_lock(Ref, S),
    {noreply, S2};

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, S) ->
    close_trees(S),
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec determine_data_root() -> string() | undefined.
determine_data_root() ->
    case ?YZ_AAE_DIR of
        {ok, EntropyRoot} ->
            EntropyRoot;
        undefined ->
            case ?DATA_DIR of
                {ok, PlatformRoot} ->
                    Root = filename:join(PlatformRoot, "yz_anti_entropy"),
                    lager:warning("Config yokozuna/anti_entropy_data_dir is "
                                  "missing. Defaulting to: ~p", [Root]),
                    application:set_env(?YZ_APP_NAME, anti_entropy_data_dir, Root),
                    Root;
                undefined ->
                    undefined
            end
    end.

%% @doc Init the trees.
%%
%% MarkEmpty is a boolean dictating whether we're marking the tree empty for the
%% the first creation or just marking it open instead.
-spec init_trees([{p(),n()}], boolean(), state()) -> state().
init_trees(RPs, MarkEmpty, S) ->
    S2 = lists:foldl(
           fun(Id, SAcc) ->
               case MarkEmpty of
                   true  -> do_new_tree(Id, SAcc, mark_empty);
                   false -> do_new_tree(Id, SAcc, mark_open)
               end
           end, S, RPs),
    S2#state{built=false, closed=false, expired=false}.

-spec load_built(state()) -> boolean().
load_built(#state{trees=Trees}) ->
    {_,Tree0} = hd(Trees),
    case hashtree:read_meta(<<"built">>, Tree0) of
        {ok, <<1>>} -> true;
        _ -> false
    end.

-spec fold_keys(p(), tree(), [index_name()]) -> [ok|timeout|not_available].
fold_keys(Partition, Tree, Indexes) ->
    LI = yz_cover:logical_index(yz_misc:get_ring(transformed)),
    LogicalPartition = yz_cover:logical_partition(LI, Partition),
    F = fun({BKey, Hash}) ->
                %% TODO: return _yz_fp from iterator and use that for
                %%       more efficient get_index_N
                IndexN = get_index_n(BKey),
                insert(async, IndexN, BKey, Hash, Tree, [if_missing])
        end,
    Filter = [{partition, LogicalPartition}],
    [yz_entropy:iterate_entropy_data(I, Filter, F) || I <- Indexes].

%% @see riak_kv_index_hashtree:do_new_tree/3
-spec do_new_tree({p(),n()}, state(), mark_open|mark_empty) -> state().
do_new_tree(Id, S=#state{trees=Trees, path=Path}, MarkType) ->
    Index = S#state.index,
    IdBin = tree_id(Id),
    NewTree0 = case Trees of
                  [] ->
                      hashtree:new({Index,IdBin}, [{segment_path, Path}]);
                  [{_,Other}|_] ->
                      hashtree:new({Index,IdBin}, Other)
               end,
    NewTree1 = case MarkType of
                   mark_empty -> hashtree:mark_open_empty(Id, NewTree0);
                   mark_open  -> hashtree:mark_open_and_check(Id, NewTree0)
               end,
    Trees2 = orddict:store(Id, NewTree1, Trees),
    S#state{trees=Trees2}.

-spec do_get_lock(term(), pid(), state()) ->
                         {ok | not_build | already_locked, state()}.
do_get_lock(_, _, S) when S#state.built /= true ->
    lager:debug("Not built: ~p", [S#state.index]),
    {not_built, S};

do_get_lock(_Type, Pid, S=#state{lock=undefined}) ->
    Ref = monitor(process, Pid),
    S2 = S#state{lock=Ref},
    {ok, S2};

do_get_lock(_, _, S) ->
    lager:debug("Already locked: ~p", [S#state.index]),
    {already_locked, S}.

-spec maybe_release_lock(reference(), state()) -> state().
maybe_release_lock(Ref, S) ->
    case S#state.lock of
        Ref -> S#state{lock=undefined};
        _ -> S
    end.

%% @private
%%
%% @doc Utility function for passing a specific hashtree into a
%%      provided function and storing the possibly-modified hashtree
%%      back in the index_hashtree state.
-spec apply_tree({p(),n()},
                 fun((hashtree()) -> {'noreply' | any(), hashtree()}),
                 state())
                -> {'reply', 'not_responsible', state()} |
                   {'reply', any(), state()} |
                   {'noreply', state()}.
apply_tree(Id, Fun, S=#state{trees=Trees}) ->
    case orddict:find(Id, Trees) of
        error ->
            {reply, not_responsible, S};
        {ok, Tree} ->
            {Result, Tree2} = Fun(Tree),
            Trees2 = orddict:store(Id, Tree2, Trees),
            S2 = S#state{trees=Trees2},
            case Result of
                noreply -> {noreply, S2};
                _ -> {reply, Result, S2}
            end
    end.

-spec do_build_finished(state()) -> state().
do_build_finished(S=#state{index=Index, built=_Pid, trees=Trees0}) ->
    lager:debug("Finished YZ build: ~p", [Index]),
    Trees = orddict:map(fun(_Id, Tree) ->
                            hashtree:flush_buffer(Tree)
                        end, Trees0),
    {_, Tree0} = hd(Trees),
    BuildTime = yz_kv:get_tree_build_time(Tree0),
    hashtree:write_meta(<<"built">>, <<1>>, Tree0),
    hashtree:write_meta(<<"build_time">>, term_to_binary(BuildTime), Tree0),
    yz_kv:update_aae_tree_stats(Index, BuildTime),
    S#state{built=true, build_time=BuildTime, expired=false, trees=Trees}.

-spec do_insert({p(),n()}, binary(), binary(), proplist(), state()) -> state().
do_insert(Id, Key, Hash, Opts, S=#state{trees=Trees}) ->
    case orddict:find(Id, Trees) of
        {ok, Tree} ->
            Tree2 = hashtree:insert(Key, Hash, Tree, Opts),
            Trees2 = orddict:store(Id, Tree2, Trees),
            S#state{trees=Trees2};
        _ ->
            handle_unexpected_key(Id, Key, S)
    end.

-spec do_delete({p(),n()}, binary(), state()) -> state().
do_delete(Id, Key, S=#state{trees=Trees}) ->
    case orddict:find(Id, Trees) of
        {ok, Tree} ->
            Tree2 = hashtree:delete(Key, Tree),
            Trees2 = orddict:store(Id, Tree2, Trees),
            S#state{trees=Trees2};
        _ ->
            handle_unexpected_key(Id, Key, S)
    end.

-spec handle_unexpected_key({p(),n()}, binary(), state()) -> state().
handle_unexpected_key(Id, Key, S=#state{index=Partition}) ->
    RP = riak_kv_util:responsible_preflists(Partition),
    case lists:member(Id, RP) of
        false ->
            %% The encountered object does not belong to any preflists that
            %% this partition is associated with. Under normal Riak operation,
            %% this should only happen when the `n_val' for an object is
            %% reduced. For example, write an object with N=3, then change N to
            %% 2. There will be an extra replica of the object that is no
            %% longer needed. We should probably just delete these objects, but
            %% to be safe rather than sorry, the first version of AAE simply
            %% ignores these objects.
            S;

        true ->
            %% The encountered object belongs to a preflist that is currently
            %% associated with this partition, but was not when the
            %% index_hashtree process was created. This occurs when increasing
            %% the `n_val' for an object. For example, write an object with N=3
            %% and it will map to the index/n preflist `{<index>, 3}'. Increase
            %% N to 4, and the object now maps to preflist '{<index>, 4}' which
            %% may not have an existing hashtree if there were previously no
            %% objects with N=4.
            lager:info("Partition/tree ~p/~p does not exist to hold object ~p",
                       [Partition, Id, Key]),
            case S#state.built of
                true ->
                    %% If the tree is already built, clear the tree to trigger
                    %% a rebuild that will re-distribute objects into the
                    %% proper hashtrees based on current N values.
                    lager:info("Clearing tree to trigger future rebuild"),
                    clear_tree(S);
                _ ->
                    %% Initialize a new index_n tree to prevent future errors.
                    %% The various hashtrees will likely be inconsistent, with
                    %% some trees containing key/hash pairs that should be in
                    %% other trees (eg. due to a change in N value). This will
                    %% be resolved whenever trees are eventually rebuilt, either
                    %% after normal expiration or after a future unexpected value
                    %% triggers the alternate case clause above.
                    do_new_tree(Id, S, mark_open)
            end
    end.

tree_id({Index, N}) ->
    %% hashtree is hardcoded for 22-byte (176-bit) tree id
    <<Index:160/integer,N:16/integer>>;
tree_id(_) ->
    erlang:error(badarg).

do_compare(Id, Remote, AccFun, Acc, From, S) ->
    case orddict:find(Id, S#state.trees) of
        error ->
            %% This case shouldn't happen, but might as well safely handle it.
            lager:warning("Tried to compare nonexistent tree "
                          "(vnode)=~p (preflist)=~p", [S#state.index, Id]),
            gen_server:reply(From, []);
        {ok, Tree} ->
            spawn_link(
              fun() ->
                      Remote(init, self()),
                      Result = hashtree:compare(Tree, Remote, AccFun, Acc),
                      Remote(final, self()),
                      gen_server:reply(From, Result)
              end)
    end,
    ok.

%% TODO: OMG cache this with entry in proc dict, use `_yz_fp' as Index
%%       and keep an orddict(Bucket,N) in proc dict
get_index_n(BKey) ->
    riak_kv_util:get_index_n(BKey).

-spec do_poke(state()) -> state().
do_poke(S) ->
    S1 = maybe_rebuild(maybe_expire(S)),
    S2 = maybe_build(S1),
    S2.

-spec maybe_expire(state()) -> state().
maybe_expire(S=#state{lock=undefined, built=true}) ->
    Diff = timer:now_diff(os:timestamp(), S#state.build_time),
    Expire = ?YZ_ENTROPY_EXPIRE,
    case (Expire /= never) andalso (Diff > (Expire * 1000))  of
        true ->  S#state{expired=true};
        false -> maybe_expire_caps_check(S)
    end;

maybe_expire(S) ->
    S.

-spec clear_tree(state()) -> state().
clear_tree(S=#state{index=Index}) ->
    lager:debug("Clearing YZ AAE tree: ~p", [S#state.index]),
    S2 = destroy_trees(S),
    IndexN = riak_kv_util:responsible_preflists(Index),
    S3 = init_trees(IndexN, true, S2#state{trees=orddict:new()}),
    ok = yz_kv:update_aae_tree_stats(Index, undefined),
    S3#state{built=false, expired=false}.

destroy_trees(S) ->
    S2 = close_trees(S),
    {_,Tree0} = hd(S#state.trees), % deliberately using state with live db ref
    hashtree:destroy(Tree0),
    S2.

maybe_build(S=#state{built=false}) ->
    Self = self(),
    Pid = spawn_link(fun() -> build_or_rehash(Self, S) end),
    S#state{built=Pid};

maybe_build(S) ->
    %% Already built or build in progress
    S.

%% @private
%%
%% @doc Flush and close the trees if not closed already.
-spec close_trees(#state{}) -> #state{}.
close_trees(S=#state{trees=Trees, closed=false}) ->
    Trees2 = [begin
                  NewTree =
                      try
                          case hashtree:next_rebuild(Tree) of
                              %% Not marking close cleanly to avoid the
                              %% cost of a full rebuild on shutdown.
                              full ->
                                  lager:info("Deliberately marking YZ hashtree ~p"
                                             ++ " for full rebuild on next restart",
                                             [IdxN]),
                                  hashtree:flush_buffer(Tree);
                              incremental ->
                                  HT = hashtree:update_tree(Tree),
                                  hashtree:mark_clean_close(IdxN, HT)
                          end
                      catch _:Err ->
                              lager:warning("Failed to flush/update trees"
                                            ++ " during close | Error: ~p", [Err]),
                              Tree
                      end,
                  {IdxN, NewTree}
              end || {IdxN, Tree} <- Trees],
    Trees3 = [{IdxN, hashtree:close(Tree)} || {IdxN, Tree} <- Trees2],
    S#state{trees=Trees3, closed=true};
close_trees(S) ->
    S.

-spec build_or_rehash(pid(), state()) -> ok.
build_or_rehash(Tree, S) ->
    Type = case load_built(S) of
               false -> build;
               true  -> rehash
           end,
    Locked = get_all_locks(Type, self()),
    build_or_rehash(Tree, Locked, Type, S).

-spec build_or_rehash(pid(), boolean(), build | rehash, state()) -> ok.
build_or_rehash(Tree, Locked, Type, #state{index=Index, trees=Trees}) ->
    case {Locked, Type} of
        {true, build} ->
            lager:debug("Starting YZ AAE tree build: ~p", [Index]),
            Indexes = yz_index:get_indexes_from_meta(),
            case yz_fuse:check_all_fuses_not_blown(Indexes) of
                true ->
                    IterKeys = fold_keys(Index, Tree, Indexes),
                    handle_iter_keys(Tree, Index, IterKeys);
                false ->
                    ?ERROR("YZ AAE did not run due to blown fuses/solr_cores."),
                    lager:debug("YZ AAE tree build failed: ~p", [Index]),
                    gen_server:cast(Tree, build_failed)
            end;
        {true, rehash} ->
            lager:debug("Starting YZ AAE tree rehash: ~p", [Index]),
            _ = [hashtree:rehash_tree(T) || {_,T} <- Trees],
            lager:debug("Finished YZ AAE tree rehash: ~p", [Index]),
            gen_server:cast(Tree, build_finished);
        {_, _} ->
            gen_server:cast(Tree, build_failed)
    end.

maybe_rebuild(S=#state{lock=undefined, built=true, expired=true}) ->
    Tree = self(),
    Pid = spawn_link(fun() ->
                             receive
                                 {lock, Locked, S2} ->
                                     build_or_rehash(Tree, Locked, build, S2);
                                 stop ->
                                     ok
                             end
                     end),
    Locked = get_all_locks(build, Pid),
    case Locked of
        true ->
            S2 = clear_tree(S),
            Pid ! {lock, Locked, S2},
            S2#state{built=Pid};
        _ ->
            Pid ! stop,
            S
    end;
maybe_rebuild(S) ->
    S.

-spec get_all_locks(build | rehash, pid()) -> boolean().
get_all_locks(Type, Pid) ->
    %% NOTE: Yokozuna diverges from KV here. KV has notion of vnode
    %% fold to make sure handoff/aae don't fight each other. Yokozuna
    %% has no vnodes. It would probably be a good idea to add a lock
    %% around Solr so that mutliple tree builds don't fight for the
    %% file page cache but the bg manager stuff is kind of convoluted
    %% and there isn't time to figure this all out for 2.0. Thus,
    %% Yokozuna will not bother with the Solr lock for now.
    ok == yz_entropy_mgr:get_lock(Type, Pid).

%% @doc Maybe expire trees for rebuild depending on riak_core_capability
%%      checks/changes. Used for possible upgrade path.
-spec maybe_expire_caps_check(state()) -> state().
maybe_expire_caps_check(S) ->
    DefaultBTCapVersion = riak_core_capability:get(
                            ?YZ_CAPS_HANDLE_LEGACY_DEFAULT_BUCKET_TYPE_AAE, v0),
    case DefaultBTCapVersion =/= v1 of
        true ->
            S#state{expired=true};
        false -> S
    end.

-spec handle_iter_keys(pid(), p(), [ok|timeout|not_available|error]) -> ok.
handle_iter_keys(Tree, Index, []) ->
    lager:debug("Finished YZ AAE tree build: ~p", [Index]),
    gen_server:cast(Tree, build_finished),
    ok;
handle_iter_keys(Tree, Index, IterKeys) ->
    case lists:all(fun(V) -> V == ok end, IterKeys)  of
        true ->
            lager:debug("Finished YZ AAE tree build: ~p", [Index]),
            gen_server:cast(Tree, build_finished);
        _ ->
            lager:debug("YZ AAE tree build failed: ~p", [Index]),
            gen_server:cast(Tree, build_failed)
    end.
