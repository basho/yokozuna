-module(yz_index_hashtree).
-behaviour(gen_server).
-include("yokozuna.hrl").

%% TODO: remove this
-include_lib("riak_kv/include/riak_kv_vnode.hrl").

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
-define(EXPIRE, 100000000).

%%%===================================================================
%%% API
%%%===================================================================

start(Index) ->
    supervisor:start_child(yz_index_hashtree_sup, [Index]).

start_link(Index) ->
    gen_server:start_link(?MODULE, [Index], []).

%% @doc Insert the given `Key' and `Hash' pair on `Tree' for the given `Id'
-spec insert({p(),n()}, binary(), binary(), tree(), list()) -> ok.
insert(Id, Key, Hash, Tree, Options) ->
    gen_server:cast(Tree, {insert, Id, Key, Hash, Options}).

%% @doc Delete the `BKey' from `Tree'.  The id will be determined from `BKey'.
-spec delete(tuple(), tree()) -> ok.
delete(BKey, Tree) ->
    gen_server:cast(Tree, {delete, BKey}).

-spec update({p(),n()}, tree()) -> ok.
update(Id, Tree) ->
    put(calling, ?LINE),
    gen_server:call(Tree, {update_tree, Id}, infinity).

%% @doc Build the trees if they need to be built.
-spec build(tree()) -> ok.
build(Tree) ->
    gen_server:cast(Tree, build).

exchange_bucket(Id, Level, Bucket, Tree) ->
    put(calling, ?LINE),
    gen_server:call(Tree, {exchange_bucket, Id, Level, Bucket}, infinity).

exchange_segment(Id, Segment, Tree) ->
    put(calling, ?LINE),
    gen_server:call(Tree, {exchange_segment, Id, Segment}, infinity).

compare(Id, Remote, Tree) ->
    compare(Id, Remote, undefined, Tree).

compare(Id, Remote, AccFun, Tree) ->
    put(calling, ?LINE),
    gen_server:call(Tree, {compare, Id, Remote, AccFun}, infinity).

get_index(Tree) ->
    gen_server:call(Tree, get_index, infinity).

get_lock(Tree, Type) ->
    get_lock(Tree, Type, self()).

get_lock(Tree, Type, Pid) ->
    put(tree, Tree),
    put(calling, ?LINE),
    gen_server:call(Tree, {get_lock, Type, Pid}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Index]) ->
    Path = filename:join(?YZ_HASHTREES_BASE_PATH, integer_to_list(Index)),
    RPs = riak_kv_vnode:responsible_preflists(Index),
    S = #state{index=Index,
               trees=orddict:new(),
               built=false,
               path=Path},
    S2 = init_trees(RPs, S),
    {ok, S2}.

-spec init_trees([{p(),n()}], state()) -> state().
init_trees(RPs, S) ->
    S2 = lists:foldl(fun(Id, SAcc) ->
                                 do_new_tree(Id, SAcc)
                         end, S, RPs),
    S2#state{built=false}.

%% TODO: If one is build they are all built?
load_built(#state{trees=Trees}) ->
    {_,Tree0} = hd(Trees),
    case hashtree:read_meta(<<"built">>, Tree0) of
        {ok, <<1>>} ->
            true;
        _ ->
            false
    end.

fold_keys(Partition, Tree) ->
    LI = yz_cover:logical_index(yz_misc:get_ring(transformed)),
    LogicalPartition = yz_cover:logical_partition(LI, Partition),
    Cores = yz_solr:cores(),
    F = fun({BKey, Hash}) ->
                %% TODO: return _yz_fp from iterator and use that for
                %%       more efficient get_index_N
                IndexN = get_index_n(BKey),
                insert(IndexN, term_to_binary(BKey), Hash, Tree, [if_missing])
        end,
    Filter = [{partition, LogicalPartition}],
    [yz_entropy:iterate_entropy_data(Core, Filter, F) || Core <- Cores],
    ok.

handle_call(get_index, _From, S) ->
    {reply, S#state.index, S};

handle_call({get_lock, Type, Pid}, _From, State) ->
    {Reply, State2} = do_get_lock(Type, Pid, State),
    {reply, Reply, State2};

handle_call({update_tree, Id}, From, S) ->
    lager:info("Updating tree for partition ~p preflist ~p",
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

handle_call({exchange_bucket, Id, Level, Bucket}, _From, State) ->
    apply_tree(Id,
               fun(Tree) ->
                       Result = hashtree:get_bucket(Level, Bucket, Tree),
                       {Result, Tree}
               end,
               State);

handle_call({exchange_segment, Id, Segment}, _From, State) ->
    apply_tree(Id,
               fun(Tree) ->
                       [{_, Result}] = hashtree:key_hashes(Tree, Segment),
                       {Result, Tree}
               end,
               State);

handle_call({compare, Id, Remote, AccFun}, From, State) ->
    Tree = orddict:fetch(Id, State#state.trees),
    spawn(fun() ->
                  Result = case AccFun of
                               undefined ->
                                   hashtree:compare(Tree, Remote);
                               _ ->
                                   hashtree:compare(Tree, Remote, AccFun)
                           end,
                  gen_server:reply(From, Result)
          end),
    {noreply, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(poke, State) ->
    State2 = do_poke(State),
    {noreply, State2};

handle_cast(build, S) ->
    S2 = maybe_build(S),
    {noreply, S2};

handle_cast(build_failed, State) ->
    gen_server:cast(entropy_manager, {requeue_poke, State#state.index}),
    State2 = State#state{built=false},
    {noreply, State2};
handle_cast(build_finished, State) ->
    State2 = do_build_finished(State),
    {noreply, State2};

handle_cast({insert, Id, Key, Hash, Options}, State) ->
    State2 = do_insert(Id, Key, Hash, Options, State),
    {noreply, State2};

handle_cast({delete, BKey}, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    IndexN = get_index_n(BKey, Ring),
    State2 = do_delete(IndexN, term_to_binary(BKey), State),
    {noreply, State2};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, _, _, _}, State) ->
    State2 = maybe_release_lock(Ref, State),
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec do_new_tree({p(),n()}, state()) -> state().
do_new_tree(Id, State=#state{trees=Trees, path=Path}) ->
    Index = State#state.index,
    IdBin = tree_id(Id),
    NewTree = case Trees of
                  [] ->
                      hashtree:new({Index,IdBin}, [{segment_path, Path}]);
                  [{_,Other}|_] ->
                      hashtree:new({Index,IdBin}, Other)
              end,
    Trees2 = orddict:store(Id, NewTree, Trees),
    State#state{trees=Trees2}.

do_get_lock(_, _, State) when State#state.built /= true ->
    lager:info("Not built: ~p :: ~p", [State#state.index, State#state.built]),
    {not_built, State};
do_get_lock(_Type, Pid, State=#state{lock=undefined}) ->
    Ref = monitor(process, Pid),
    State2 = State#state{lock=Ref},
    %% lager:info("Locked: ~p", [State#state.index]),
    {ok, State2};
do_get_lock(_, _, State) ->
    lager:info("Already locked: ~p", [State#state.index]),
    {already_locked, State}.

maybe_release_lock(Ref, State) ->
    case State#state.lock of
        Ref ->
            %% lager:info("Unlocked: ~p", [State#state.index]),
            State#state{lock=undefined};
        _ ->
            State
    end.

apply_tree(Id, Fun, State=#state{trees=Trees}) ->
    case orddict:find(Id, Trees) of
        error ->
            {reply, not_responsible, State};
        {ok, Tree} ->
            {Result, Tree2} = Fun(Tree),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State2 = State#state{trees=Trees2},
            case Result of
                noreply ->
                    {noreply, State2};
                _ ->
                    {reply, Result, State2}
            end
    end.

do_build_finished(State=#state{index=Index, built=_Pid}) ->
    lager:info("Finished build (b): ~p", [Index]),
    {_,Tree0} = hd(State#state.trees),
    hashtree:write_meta(<<"built">>, <<1>>, Tree0),
    State#state{built=true, build_time=os:timestamp()}.

do_insert(Id, Key, Hash, Opts, State=#state{trees=Trees}) ->
    %% lager:info("Insert into ~p/~p :: ~p / ~p", [State#state.index, Id, Key, Hash]),
    case orddict:find(Id, Trees) of
        {ok, Tree} ->
            Tree2 = hashtree:insert(Key, Hash, Tree, Opts),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State#state{trees=Trees2};
        _ ->
            State2 = handle_unexpected_key(Id, Key, State),
            State2
    end.

do_delete(Id, Key, State=#state{trees=Trees}) ->
    case orddict:find(Id, Trees) of
        {ok, Tree} ->
            Tree2 = hashtree:delete(Key, Tree),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State#state{trees=Trees2};
        _ ->
            State2 = handle_unexpected_key(Id, Key, State),
            State2
    end.

handle_unexpected_key(Id, Key, State=#state{index=Partition}) ->
    RP = riak_kv_vnode:responsible_preflists(Partition),
    case lists:member(Id, RP) of
        false ->
            lager:warning("Object ~p encountered during fold over partition "
                          "~p, but key does not hash to an index handled by "
                          "this partition", [Key, Partition]),
            State;
        true ->
            lager:info("Partition/tree ~p/~p does not exist to hold object ~p",
                       [Partition, Id, Key]),
            case State#state.built of
                true ->
                    lager:info("Clearing tree to trigger future rebuild"),
                    clear_tree(State);
                _ ->
                    %% Initialize new index_n tree to prevent future errors,
                    %% but state may be inconsistent until future rebuild
                    State2 = do_new_tree(Id, State),
                    State2
            end
    end.

tree_id({Index, N}) ->
    %% hashtree is hardcoded for 22-byte (176-bit) tree id
    <<Index:160/integer,N:16/integer>>;
tree_id(_) ->
    erlang:error(badarg).

%% TODO: OMG cache this with entry in proc dict, use `_yz_fp` as Index
%%       and keep an orddict(Bucket,N) in proc dict
get_index_n(BKey) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    get_index_n(BKey, Ring).

get_index_n({Bucket, Key}, Ring) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    N = proplists:get_value(n_val, BucketProps),
    ChashKey = riak_core_util:chash_key({Bucket, Key}),
    Index = riak_core_ring:responsible_index(ChashKey, Ring),
    {Index, N}.

do_poke(State) ->
    State1 = maybe_clear(State),
    State2 = maybe_build(State1),
    State2.

maybe_clear(State=#state{lock=undefined, built=true}) ->
    Diff = timer:now_diff(os:timestamp(), State#state.build_time),
    case Diff > ?EXPIRE of
        true ->
            clear_tree(State);
        false ->
            State
    end;
maybe_clear(State) ->
    State.

clear_tree(S=#state{index=Index, trees=Trees}) ->
    lager:info("Clearing tree ~p", [S#state.index]),
    {_,Tree0} = hd(Trees),
    hashtree:destroy(Tree0),
    IndexN = riak_kv_vnode:responsible_preflists(Index),
    S2 = init_trees(IndexN, S#state{trees=orddict:new()}),
    S2#state{built=false}.

maybe_build(S=#state{built=false}) ->
    Self = self(),
    Pid = spawn_link(fun() ->
                             case yz_entropy_mgr:get_lock(build) of
                                 max_concurrency ->
                                     gen_server:cast(Self, build_failed);
                                 ok ->
                                     build_or_rehash(Self, S)
                             end
                     end),
    S#state{built=Pid};
maybe_build(S) ->
    %% Already built or build in progress
    S.

build_or_rehash(Self, S=#state{index=Index, trees=Trees}) ->
    case load_built(S) of
        false ->
            lager:info("Starting build: ~p", [Index]),
            fold_keys(Index, Self),
            lager:info("Finished build (a): ~p", [Index]),
            gen_server:cast(Self, build_finished);
        true ->
            lager:info("Starting rehash: ~p", [Index]),
            _ = [hashtree:rehash_tree(T) || {_,T} <- Trees],
            lager:info("Finished rehash (a): ~p", [Index]),
            gen_server:cast(Self, build_finished)
    end.
