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
                lock :: undefined | reference(),
                path,
                build_time,
                trees}).
-type state() :: #state{}.

-compile(export_all).

%% Time from build to expiration of tree, in microseconds.
-define(DEFAULT_EXPIRE, 604800000). %% 1 week

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
    catch gen_server:call(Tree, {insert, Id, BKey, Hash, Options}, 1000).

%% @doc Delete the `BKey' from `Tree'.  The id will be determined from
%%      `BKey'.  The result of the sync call should be ignored since
%%      it uses catch.
-spec delete(async | sync, {p(),n()}, bkey(), tree()) -> ok.
delete(async, Id, BKey, Tree) ->
    gen_server:cast(Tree, {delete, Id, BKey});

delete(sync, Id, BKey, Tree) ->
    catch gen_server:call(Tree, {delete, Id, BKey}, 1000).

-spec update({p(),n()}, tree()) -> ok.
update(Id, Tree) ->
    gen_server:call(Tree, {update_tree, Id}, infinity).

-spec compare({p(),n()}, hashtree:remote_fun(), tree()) ->
                     [hashtree:keydiff()].
compare(Id, Remote, Tree) ->
    compare(Id, Remote, undefined, Tree).

-spec compare({p(),n()}, hashtree:remote_fun(),
              undefined | hashtree:acc_fun(T), tree()) -> T.
compare(Id, Remote, AccFun, Tree) ->
    gen_server:call(Tree, {compare, Id, Remote, AccFun}, infinity).

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

%% @doc Terminate the `Tree'.
stop(Tree) ->
    gen_server:cast(Tree, stop).

%% @doc Destroy the specified `Tree', which will destroy all
%%      associated hashtrees and terminate.
-spec destroy(tree()) -> ok.
destroy(Tree) ->
    gen_server:call(Tree, destroy, infinity).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Index, RPs]) ->
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
                       path=Path},
            S2 = init_trees(RPs, S),
            {ok, S2}
    end.

handle_call({insert, Id, BKey, Hash, Options}, _From, S) ->
    S2 = do_insert(Id, term_to_binary(BKey), Hash, Options, S),
    {reply, ok, S2};

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
                       spawn_link(fun() ->
                                          hashtree:update_perform(SnapTree),
                                          gen_server:reply(From, ok)
                                  end),
                       {noreply, Tree2}
               end,
               S);

handle_call({compare, Id, Remote, AccFun}, From, S) ->
    do_compare(Id, Remote, AccFun, From, S),
    {noreply, S};

handle_call(destroy, _From, S) ->
    S2 = destroy_trees(S),
    {stop, normal, ok, S2};

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

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({'DOWN', Ref, _, _, _}, S) ->
    S2 = maybe_release_lock(Ref, S),
    {noreply, S2};

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec determine_data_root() -> string() | undefined.
determine_data_root() ->
    case ?YZ_AE_DIR of
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

-spec init_trees([{p(),n()}], state()) -> state().
init_trees(RPs, S) ->
    S2 = lists:foldl(fun(Id, SAcc) ->
                                 do_new_tree(Id, SAcc)
                         end, S, RPs),
    S2#state{built=false}.

-spec load_built(state()) -> boolean().
load_built(#state{trees=Trees}) ->
    {_,Tree0} = hd(Trees),
    case hashtree:read_meta(<<"built">>, Tree0) of
        {ok, <<1>>} -> true;
        _ -> false
    end.

-spec fold_keys(p(), tree()) -> ok.
fold_keys(Partition, Tree) ->
    LI = yz_cover:logical_index(yz_misc:get_ring(transformed)),
    LogicalPartition = yz_cover:logical_partition(LI, Partition),
    Indexes = yz_index:get_indexes_from_ring(yz_misc:get_ring(transformed)),
    Indexes2 = [{?YZ_DEFAULT_INDEX, ignored}|Indexes],
    F = fun({BKey, Hash}) ->
                %% TODO: return _yz_fp from iterator and use that for
                %%       more efficient get_index_N
                IndexN = get_index_n(BKey),
                insert(async, IndexN, BKey, Hash, Tree, [if_missing])
        end,
    Filter = [{partition, LogicalPartition}],
    [yz_entropy:iterate_entropy_data(Name, Filter, F) || {Name,_} <- Indexes2],
    ok.

-spec do_new_tree({p(),n()}, state()) -> state().
do_new_tree(Id, S=#state{trees=Trees, path=Path}) ->
    Index = S#state.index,
    IdBin = tree_id(Id),
    NewTree = case Trees of
                  [] ->
                      hashtree:new({Index,IdBin}, [{segment_path, Path}]);
                  [{_,Other}|_] ->
                      hashtree:new({Index,IdBin}, Other)
              end,
    Trees2 = orddict:store(Id, NewTree, Trees),
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
do_build_finished(S=#state{index=Index, built=_Pid}) ->
    lager:debug("Finished build: ~p", [Index]),
    {_,Tree0} = hd(S#state.trees),
    BuildTime = yz_kv:get_tree_build_time(Tree0),
    hashtree:write_meta(<<"built">>, <<1>>, Tree0),
    hashtree:write_meta(<<"build_time">>, term_to_binary(BuildTime), Tree0),
    yz_kv:update_aae_tree_stats(Index, BuildTime),
    S#state{built=true, build_time=BuildTime}.

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
                    do_new_tree(Id, S)
            end
    end.

tree_id({Index, N}) ->
    %% hashtree is hardcoded for 22-byte (176-bit) tree id
    <<Index:160/integer,N:16/integer>>;
tree_id(_) ->
    erlang:error(badarg).

%% TODO: handle non-existent tree
do_compare(Id, Remote, AccFun, From, S) ->
    case orddict:find(Id, S#state.trees) of
        error ->
            %% This case shouldn't happen, but might as well safely handle it.
            lager:warning("Tried to compare nonexistent tree "
                          "(vnode)=~p (preflist)=~p", [S#state.index, Id]),
            gen_server:reply(From, []);
        {ok, Tree} ->
            spawn_link(
              fun() ->
                      Result = case AccFun of
                                   undefined ->
                                       hashtree:compare(Tree, Remote);
                                   _ ->
                                       hashtree:compare(Tree, Remote, AccFun)
                               end,
                      gen_server:reply(From, Result)
              end)
    end,
    ok.

%% TODO: OMG cache this with entry in proc dict, use `_yz_fp` as Index
%%       and keep an orddict(Bucket,N) in proc dict
get_index_n(BKey) ->
    get_index_n(BKey, yz_misc:get_ring(transformed)).

get_index_n({Bucket, Key}, Ring) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    N = proplists:get_value(n_val, BucketProps),
    ChashKey = riak_core_util:chash_key({Bucket, Key}),
    Index = riak_core_ring:responsible_index(ChashKey, Ring),
    {Index, N}.

do_poke(S) ->
    maybe_build(maybe_clear(S)).

maybe_clear(S=#state{lock=undefined, built=true}) ->
    Diff = timer:now_diff(os:timestamp(), S#state.build_time),
    Expire = app_helper:get_env(riak_kv,
                                anti_entropy_expire,
                                ?DEFAULT_EXPIRE),
    case Diff > (Expire * 1000)  of
        true -> clear_tree(S);
        false -> S
    end;

maybe_clear(S) ->
    S.

clear_tree(S=#state{index=Index}) ->
    lager:debug("Clearing tree ~p", [S#state.index]),
    S2 = destroy_trees(S),
    IndexN = riak_kv_util:responsible_preflists(Index),
    S3 = init_trees(IndexN, S2#state{trees=orddict:new()}),
    S3#state{built=false}.

destroy_trees(S) ->
    S2 = close_trees(S),
    {_,Tree0} = hd(S2#state.trees),
    hashtree:destroy(Tree0),
    S2.

maybe_build(S=#state{built=false}) ->
    Self = self(),
    Pid = spawn_link(fun() -> build_or_rehash(Self, S) end),
    S#state{built=Pid};

maybe_build(S) ->
    %% Already built or build in progress
    S.

close_trees(S=#state{trees=Trees}) ->
    Trees2 = [{IdxN, hashtree:close(Tree)} || {IdxN, Tree} <- Trees],
    S#state{trees=Trees2}.

build_or_rehash(Self, S=#state{index=Index, trees=Trees}) ->
    Type = case load_built(S) of
               false -> build;
               true  -> rehash
           end,
    Lock = yz_entropy_mgr:get_lock(Type),
    case {Lock, Type} of
        {ok, build} ->
            lager:debug("Starting build: ~p", [Index]),
            fold_keys(Index, Self),
            lager:debug("Finished build: ~p", [Index]),
            gen_server:cast(Self, build_finished);
        {ok, rehash} ->
            lager:debug("Starting rehash: ~p", [Index]),
            _ = [hashtree:rehash_tree(T) || {_,T} <- Trees],
            lager:debug("Finished rehash: ~p", [Index]),
            gen_server:cast(Self, build_finished);
        {_Error, _} ->
            gen_server:cast(Self, build_failed)
    end.
