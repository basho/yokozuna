-module(hashtree).
-export([new/0,
         insert/3,
         update_tree/1,
         destroy/1,
         local_compare/2,
         compare/2]).
-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NUM_SEGMENTS, (1024*1024)).
-define(WIDTH, 1024).
-define(MEM_LEVELS, 4).
-define(ROOT, "/tmp/anti/level").

-record(state, {id,
                levels,
                tree,
                ref,
                path,
                itr,
                dirty_segments}).

%%%===================================================================
%%% API
%%%===================================================================

new() ->
    new(0).

new(TreeId) ->
    State = new_segment_store(#state{}),
    new(TreeId, State).

new(TreeId, LinkedStore) ->
    if is_integer(TreeId) andalso
       (TreeId >= 0) andalso
       (TreeId < ((1 bsl 160)-1)) ->
            ok;
       true ->
            erlang:error(badarg)
    end,
    NumLevels = erlang:trunc(math:log(?NUM_SEGMENTS) / math:log(?WIDTH)) + 1,
    State = #state{id=TreeId,
                   levels=NumLevels,
                   %% dirty_segments=gb_sets:new(),
                   dirty_segments=bitarray_new(?NUM_SEGMENTS),
                   tree=dict:new()},
    State2 = share_segment_store(State, LinkedStore),
    State2.

destroy(State) ->
    eleveldb:destroy(State#state.path, []),
    State.

insert(Key, ObjHash, State) ->
    Hash = erlang:phash2(Key),
    Segment = Hash rem ?NUM_SEGMENTS,
    HKey = encode(State#state.id, Segment, Key),
    ok = eleveldb:put(State#state.ref, HKey, ObjHash, []),
    %% Dirty = gb_sets:add_element(Segment, State#state.dirty_segments),
    Dirty = bitarray_set(Segment, State#state.dirty_segments),
    State#state{dirty_segments=Dirty}.

update_tree(State=#state{dirty_segments=Dirty}) ->
    %% Segments = gb_sets:to_list(Dirty),
    Segments = bitarray_to_list(Dirty),
    State2 = snapshot(State),
    State3 = update_tree(Segments, State2),
    %% State3#state{dirty_segments=gb_sets:new()}.
    State3#state{dirty_segments=bitarray_new(?NUM_SEGMENTS)}.

update_tree([], State) ->
    State;
update_tree(Segments, State) ->
    Hashes = orddict:from_list(hashes(State, Segments)),
    Groups = group(Hashes),
    LastLevel = State#state.levels,
    NewState = update_levels(LastLevel, Groups, State),
    NewState.

top_hash(State) ->
    get_bucket(1, 0, State).

local_compare(T1, T2) ->
    Remote = fun(get_bucket, {L, B}) ->
                     get_bucket(L, B, T2);
                (key_hashes, Segment) ->
                     [{_, KeyHashes2}] = key_hashes(T2, Segment),
                     KeyHashes2
             end,
    compare(T1, Remote).

compare(Tree, Remote) ->
    compare(1, 0, Tree, Remote, []).

%%%===================================================================
%%% Internal functions
%%%===================================================================

new_segment_store(State) ->
    Options = [{create_if_missing, true},
               {max_open_files, 20}],
    <<P:128/integer>> = crypto:md5(term_to_binary(erlang:now())),
    DataDir = filename:join(?ROOT, integer_to_list(P)),
    filelib:ensure_dir(DataDir),
    {ok, Ref} = eleveldb:open(DataDir, Options),
    State#state{ref=Ref, path=DataDir}.

share_segment_store(State, #state{ref=Ref, path=Path}) ->
    State#state{ref=Ref, path=Path}.

hash(X) ->
    %% erlang:phash2(X).
    crypto:sha(term_to_binary(X)).

update_levels(0, _, State) ->
    State;
update_levels(Level, Groups, State) ->
    {NewState, NewBuckets} =
        lists:foldl(fun({Bucket, NewHashes}, {StateAcc, BucketsAcc}) ->
                            Hashes1 = get_bucket(Level, Bucket, StateAcc),
                            Hashes2 = orddict:from_list(NewHashes),
                            Hashes3 = orddict:merge(fun(_, _, New) -> New end,
                                                    Hashes1,
                                                    Hashes2),
                            StateAcc2 = set_bucket(Level, Bucket, Hashes3, StateAcc),
                            NewBucket = {Bucket, hash(Hashes3)},
                            {StateAcc2, [NewBucket | BucketsAcc]}
                    end, {State, []}, Groups),
    Groups2 = group(NewBuckets),
    update_levels(Level - 1, Groups2, NewState).

group(L) ->
    {FirstId, _} = hd(L),
    FirstBucket = FirstId div ?WIDTH,
    {LastBucket, LastGroup, Groups} =
        lists:foldl(fun(X={Id, _}, {LastBucket, Acc, Groups}) ->
                            Bucket = Id div ?WIDTH,
                            case Bucket of
                                LastBucket ->
                                    {LastBucket, [X|Acc], Groups};
                                _ ->
                                    {Bucket, [X], [{LastBucket, Acc} | Groups]}
                            end
                    end, {FirstBucket, [], []}, L),
    [{LastBucket, LastGroup} | Groups].

get_bucket(Level, Bucket, State) ->
    case Level =< ?MEM_LEVELS of
        true ->
            get_memory_bucket(Level, Bucket, State);
        false ->
            get_disk_bucket(Level, Bucket, State)
    end.

set_bucket(Level, Bucket, Val, State) ->
    case Level =< ?MEM_LEVELS of
        true ->
            set_memory_bucket(Level, Bucket, Val, State);
        false ->
            set_disk_bucket(Level, Bucket, Val, State)
    end.

get_memory_bucket(Level, Bucket, #state{tree=Tree}) ->
    case dict:find({Level, Bucket}, Tree) of
        error ->
            orddict:new();
        {ok, Val} ->
            Val
    end.

set_memory_bucket(Level, Bucket, Val, State) ->
    Tree = dict:store({Level, Bucket}, Val, State#state.tree),
    State#state{tree=Tree}.

get_disk_bucket(Level, Bucket, #state{id=Id, ref=Ref}) ->
    HKey = encode_bucket(Id, Level, Bucket),
    case eleveldb:get(Ref, HKey, []) of
        {ok, Bin} ->
            binary_to_term(Bin);
        _ ->
            orddict:new()
    end.

set_disk_bucket(Level, Bucket, Val, State=#state{id=Id, ref=Ref}) ->
    HKey = encode_bucket(Id, Level, Bucket),
    Bin = term_to_binary(Val),
    eleveldb:put(Ref, HKey, Bin, []),
    State.

encode(TreeId, Segment, Key) ->
    <<TreeId:160/integer,$s,Segment:64/integer,Key/binary>>.

decode(Bin) ->
    <<TreeId:160/integer,$s,Segment:64/integer,Key/binary>> = Bin,
    {TreeId, Segment, Key}.

encode_bucket(TreeId, Level, Bucket) ->
    <<TreeId:160/integer,$b,Level:64/integer,Bucket:64/integer>>.

hashes(State, Segments) ->
    multi_select_segment(State, Segments, fun hash/1).

key_hashes(State, Segment) ->
    multi_select_segment(State, [Segment], fun(X) -> X end).

snapshot(State) ->
    %% Abuse eleveldb iterators as snapshots
    catch eleveldb:iterator_close(State#state.itr),
    {ok, Itr} = eleveldb:iterator(State#state.ref, []),
    State#state{itr=Itr}.

multi_select_segment(#state{id=Id, itr=Itr}, Segments, F) ->
    [First | Rest] = Segments,
    Acc0 = {Itr, Id, First, Rest, F, [], []},
    Seek = encode(Id, First, <<>>),
    {_, _, LastSegment, _, _, LastAcc, FA} =
        iterate(eleveldb:iterator_move(Itr, Seek), Acc0),
    [{LastSegment, F(LastAcc)} | FA].

iterate({error, invalid_iterator}, AllAcc) ->
    AllAcc;
iterate({ok, K, V}, AllAcc={Itr, Id, Segment, Segments, F, Acc, FinalAcc}) ->
    {SegId, Seg, _} = decode(K),
    case {SegId, Seg, Segments} of
        {Id, Segment, _} ->
            %% Still reading existing segment
            Acc2 = {Itr, Id, Segment, Segments, F, [{K,V} | Acc], FinalAcc},
            iterate(eleveldb:iterator_move(Itr, next), Acc2);
        {Id, _, [Seg|Remaining]} ->
            %% Pointing at next segment we are interested in
            Acc2 = {Itr, Id, Seg, Remaining, F, [{K,V}], [{Segment, F(Acc)} | FinalAcc]},
            iterate(eleveldb:iterator_move(Itr, next), Acc2);
        {Id, _, [NextSeg|Remaining]} ->
            %% Pointing at uninteresting segment, seek to next interesting one
            Acc2 = {Itr, Id, NextSeg, Remaining, F, [], [{Segment, F(Acc)} | FinalAcc]},
            Seek = encode(Id, NextSeg, <<>>),
            iterate(eleveldb:iterator_move(Itr, Seek), Acc2);
        _ ->
            %% Done with traversal
            AllAcc
    end.

compare(Level, Bucket, Tree, Remote, KeyAcc) when Level == Tree#state.levels+1 ->
    Keys = compare_segments(Bucket, Tree, Remote),
    Keys ++ KeyAcc;
compare(Level, Bucket, Tree, Remote, KeyAcc) ->
    HL1 = get_bucket(Level, Bucket, Tree),
    HL2 = Remote(get_bucket, {Level, Bucket}),
    Union = lists:ukeysort(1, HL1 ++ HL2),
    Inter = ordsets:intersection(ordsets:from_list(HL1),
                                 ordsets:from_list(HL2)),
    Diff = ordsets:subtract(Union, Inter),
    KeyAcc3 =
        lists:foldl(fun({Bucket2, _}, KeyAcc2) ->
                            compare(Level+1, Bucket2, Tree, Remote, KeyAcc2)
                    end, KeyAcc, Diff),
    KeyAcc3.

compare_segments(Segment, Tree=#state{id=Id}, Remote) ->
    [{_, KeyHashes1}] = key_hashes(Tree, Segment),
    KeyHashes2 = Remote(key_hashes, Segment),
    HL1 = orddict:from_list(KeyHashes1),
    HL2 = orddict:from_list(KeyHashes2),
    Delta = orddict_delta(HL1, HL2),
    Keys =
        orddict:fold(fun(KBin, {'$none', _}, Acc) ->
                             {Id, Segment, Key} = decode(KBin),
                             [{missing, Key} | Acc];
                        (KBin, {_, '$none'}, Acc) ->
                             {Id, Segment, Key} = decode(KBin),
                             [{remote_missing, Key} | Acc];
                        (KBin, _, Acc) ->
                             {Id, Segment, Key} = decode(KBin),
                             [{different, Key} | Acc]
                     end, [], Delta),
    Keys.

orddict_delta(D1, D2) ->
    orddict_delta(D1, D2, []).

orddict_delta([{K1,V1}|D1], [{K2,_}=E2|D2], Acc) when K1 < K2 ->
    Acc2 = [{K1,{V1,'$none'}} | Acc],
    orddict_delta(D1, [E2|D2], Acc2);
orddict_delta([{K1,_}=E1|D1], [{K2,V2}|D2], Acc) when K1 > K2 ->
    Acc2 = [{K2,{'$none',V2}} | Acc],
    orddict_delta([E1|D1], D2, Acc2);
orddict_delta([{K1,V1}|D1], [{_K2,V2}|D2], Acc) -> %K1 == K2
    case V1 of
        V2 ->
            orddict_delta(D1, D2, Acc);
        _ ->
            Acc2 = [{K1,{V1,V2}} | Acc],
            orddict_delta(D1, D2, Acc2)
    end;
orddict_delta([], D2, Acc) ->
    L = [{K2,{'$none',V2}} || {K2,V2} <- D2],
    L ++ Acc;
orddict_delta(D1, [], Acc) ->
    L = [{K1,{V1,'$none'}} || {K1,V1} <- D1],
    L ++ Acc.

%%%===================================================================
%%% bitarray
%%%===================================================================
-define(W, 27).

bitarray_new(N) -> array:new((N-1) div ?W + 1, {default, 0}).

bitarray_set(I, A) ->
    AI = I div ?W,
    V = array:get(AI, A),
    V1 = V bor (1 bsl (I rem ?W)),
    array:set(AI, V1, A).

bitarray_get(I, A) ->
    AI = I div ?W,
    V = array:get(AI, A),
    V band (1 bsl (I rem ?W)) =/= 0.

bitarray_to_list(A) ->
    lists:reverse(
      array:sparse_foldl(fun(I, V, Acc) ->
                                 expand(V, I * ?W, Acc)
                         end, [], A)).

expand(0, _, Acc) ->
    Acc;
expand(V, N, Acc) ->
    Acc2 =
        case (V band 1) of
            1 ->
                [N|Acc];
            0 ->
                Acc
        end,
    expand(V bsr 1, N+1, Acc2).

%%%===================================================================
%%% Experiments
%%%===================================================================
run_local() ->
    run_local(10000).
run_local(N) ->
    timer:tc(fun do_local/1, [N]).

run_concurrent_build() ->
    run_concurrent_build(10000).
run_concurrent_build(N) ->
    run_concurrent_build(N, N).
run_concurrent_build(N1, N2) ->
    timer:tc(fun do_concurrent_build/2, [N1, N2]).

run_multiple(Count, N) ->
    Tasks = [fun() ->
                     do_concurrent_build(N, N)
             end || _ <- lists:seq(1, Count)],
    timer:tc(fun peval/1, [Tasks]).

run_remote() ->
    run_remote(100000).
run_remote(N) ->
    timer:tc(fun do_remote/1, [N]).

do_local() ->
    do_local(100000).
do_local(N) ->
    A0 = insert_many(N, new()),
    A1 = insert(<<"10">>, <<"42">>, A0),
    A2 = insert(<<"10">>, <<"42">>, A1),
    A3 = insert(<<"13">>, <<"52">>, A2),

    B0 = insert_many(N, new()),
    B1 = insert(<<"14">>, <<"52">>, B0),
    B2 = insert(<<"10">>, <<"32">>, B1),
    B3 = insert(<<"10">>, <<"422">>, B2),

    A4 = update_tree(A3),
    B4 = update_tree(B3),
    KeyDiff = local_compare(A4, B4),
    io:format("KeyDiff: ~p~n", [KeyDiff]),
    destroy(A4),
    destroy(B4),
    ok.

do_concurrent_build(N1, N2) ->
    F1 = fun() ->
                 A0 = insert_many(N1, new()),
                 A1 = insert(<<"10">>, <<"42">>, A0),
                 A2 = insert(<<"10">>, <<"42">>, A1),
                 A3 = insert(<<"13">>, <<"52">>, A2),
                 A4 = update_tree(A3),
                 A4
         end,

    F2 = fun() ->
                 B0 = insert_many(N2, new()),
                 B1 = insert(<<"14">>, <<"52">>, B0),
                 B2 = insert(<<"10">>, <<"32">>, B1),
                 B3 = insert(<<"10">>, <<"422">>, B2),
                 B4 = update_tree(B3),
                 B4
         end,

    [A4, B4] = peval([F1, F2]),
    KeyDiff = local_compare(A4, B4),
    io:format("KeyDiff: ~p~n", [KeyDiff]),

    destroy(A4),
    destroy(B4),
    ok.

do_remote(N) ->
    %% Spawn new process for remote tree
    Other =
        spawn(fun() ->
                      A0 = insert_many(N, new()),
                      A1 = insert(<<"10">>, <<"42">>, A0),
                      A2 = insert(<<"10">>, <<"42">>, A1),
                      A3 = insert(<<"13">>, <<"52">>, A2),
                      A4 = update_tree(A3),
                      message_loop(A4, 0, 0)
              end),

    %% Build local tree
    B0 = insert_many(N, new()),
    B1 = insert(<<"14">>, <<"52">>, B0),
    B2 = insert(<<"10">>, <<"32">>, B1),
    B3 = insert(<<"10">>, <<"422">>, B2),
    B4 = update_tree(B3),

    %% Compare with remote tree through message passing
    Remote = fun(get_bucket, {L, B}) ->
                     Other ! {get_bucket, self(), L, B},
                     receive {remote, X} -> X end;
                (key_hashes, Segment) ->
                     Other ! {key_hashes, self(), Segment},
                     receive {remote, X} -> X end
             end,
    KeyDiff = compare(B4, Remote),
    io:format("KeyDiff: ~p~n", [KeyDiff]),

    %% Signal spawned process to print stats and exit
    Other ! done,
    ok.

message_loop(Tree, Msgs, Bytes) ->
    receive
        {get_bucket, From, L, B} ->
            Reply = get_bucket(L, B, Tree),
            From ! {remote, Reply},
            Size = byte_size(term_to_binary(Reply)),
            message_loop(Tree, Msgs+1, Bytes+Size);
        {key_hashes, From, Segment} ->
            [{_, KeyHashes2}] = key_hashes(Tree, Segment),
            Reply = KeyHashes2,
            From ! {remote, Reply},
            Size = byte_size(term_to_binary(Reply)),
            message_loop(Tree, Msgs+1, Bytes+Size);
        done ->
            io:format("Exchanged messages: ~b~n", [Msgs]),
            io:format("Exchanged bytes:    ~b~n", [Bytes]),
            ok
    end.

insert_many(N, T1) ->
    T2 =
        lists:foldl(fun(X, TX) ->
                            insert(bin(-X), bin(X*100), TX)
                    end, T1, lists:seq(1,N)),
    T2.

bin(X) ->
    list_to_binary(integer_to_list(X)).

peval(L) ->
    Parent = self(),
    lists:foldl(
      fun(F, N) ->
              spawn(fun() ->
                            Parent ! {peval, N, F()}
                    end),
              N+1
      end, 0, L),
    L2 = [receive {peval, N, R} -> {N,R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

%%%===================================================================
%%% EUnit
%%%===================================================================

%% Verify that `update_tree/1' generates a snapshot of the underlying
%% LevelDB store that is used by `compare', therefore isolating the
%% compare from newer/concurrent insertions into the tree.
snapshot_test() ->
    A0 = insert(<<"10">>, <<"42">>, new()),
    B0 = insert(<<"10">>, <<"52">>, new()),
    A1 = update_tree(A0),
    B1 = update_tree(B0),
    B2 = insert(<<"10">>, <<"42">>, B1),
    KeyDiff = local_compare(A1, B1),
    destroy(A1),
    destroy(B2),
    ?assertEqual([{different, <<"10">>}], KeyDiff),
    ok.

%%%===================================================================
%%% EQC
%%%===================================================================

objects() ->
    ?SIZED(Size, objects(Size+3)).

objects(N) ->
    ?LET(Keys, shuffle(lists:seq(1,N)),
         [{bin(K), binary(8)} || K <- Keys]
        ).

lengths(N) ->
    ?LET(MissingN1,  choose(0,N),
    ?LET(MissingN2,  choose(0,N-MissingN1),
    ?LET(DifferentN, choose(0,N-MissingN1-MissingN2),
      {MissingN1, MissingN2, DifferentN}))).

mutate(Binary) ->
    L1 = binary_to_list(Binary),
    [X|Xs] = L1,
    X2 = (X+1) rem 256,
    L2 = [X2|Xs],
    list_to_binary(L2).

prop_correct() ->
    ?FORALL(Objects, objects(),
    ?FORALL({MissingN1, MissingN2, DifferentN}, lengths(length(Objects)),
            begin
                {RemoteOnly, Objects2} = lists:split(MissingN1, Objects),
                {LocalOnly,  Objects3} = lists:split(MissingN2, Objects2),
                {Different,  Same}     = lists:split(DifferentN, Objects3),

                Different2 = [{Key, mutate(Hash)} || {Key, Hash} <- Different],

                Insert = fun(Tree, Vals) ->
                                 lists:foldl(fun({Key, Hash}, Acc) ->
                                                     insert(Key, Hash, Acc)
                                             end, Tree, Vals)
                         end,

                A0 = new(),
                B0 = new(),

                [begin
                  A1 = new(Id, A0),
                  B1 = new(Id, B0),

                  A2 = Insert(A1, Same),
                  A3 = Insert(A2, LocalOnly),
                  A4 = Insert(A3, Different),

                  B2 = Insert(B1, Same),
                  B3 = Insert(B2, RemoteOnly),
                  B4 = Insert(B3, Different2),

                  A5 = update_tree(A4),
                  B5 = update_tree(B4),

                  Expected =
                      [{missing, Key}        || {Key, _} <- RemoteOnly] ++
                      [{remote_missing, Key} || {Key, _} <- LocalOnly] ++
                      [{different, Key}      || {Key, _} <- Different],

                  KeyDiff = local_compare(A5, B5),
                
                  ?assertEqual(lists:usort(Expected),
                               lists:usort(KeyDiff)),

                  %% Reconcile trees
                  A6 = Insert(A5, RemoteOnly),
                  B6 = Insert(B5, LocalOnly),
                  B7 = Insert(B6, Different),
                  A7 = update_tree(A6),
                  B8 = update_tree(B7),
                  ?assertEqual([], local_compare(A7, B8)),
                  true
                 end || Id <- lists:seq(0, 10)],
                destroy(A0),
                destroy(B0),
                true
            end)).
