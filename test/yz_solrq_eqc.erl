%% Property
%% If SOLR accepted batch, all should be added to AAE
%% If SOLR failed batch, none should be added.

-module(yz_solrq_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").

-include_lib("eunit/include/eunit.hrl").

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
-compile([export_all, {parse_transform, pulse_instrument}]).
-compile({pulse_replace_module, [{gen_server, pulse_gen_server}]}).

-define(QC_OUT(P),
    eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

%%
%% EUinit tests
%%

solrq_test_() ->
    {setup,
        fun() ->
            error_logger:tty(false),
            pulse:start()
        end,
        fun(_) ->
            pulse:stop(),
            error_logger:tty(true)
        end,
        {timeout, 60,
            fun() ->
                ?assert(eqc:quickcheck(?QC_OUT(eqc:testing_time(30, prop_ok()))))
            end
        }
    }.


%%
%% functions for running manually via the shell
%%

run() ->
    run(10).

run(Secs) ->
    eqc:quickcheck(eqc:testing_time(Secs, prop_ok())).

check() ->
    eqc:check(prop_ok()).

recheck() ->
    eqc:recheck(prop_ok()).

cover(Secs) ->
    cover:compile_beam(yz_solrq),
    cover:compile_beam(yz_solrq_helper),
    eqc:quickcheck(eqc:testing_time(Secs, prop_ok())),
    cover:analyse_to_file(yz_solrq,[html]),
    cover:analyse_to_file(yz_solrq_helper,[html]).


%%
%% Generators
%%

gen_partition() ->
    nat().

-ifndef(YZ_INDEX_TOMBSTONE).
-define(YZ_INDEX_TOMBSTONE, <<"_dont_index_">>).
-endif.
gen_index() ->
    oneof([<<"idx1">>, <<"idx2">>, ?YZ_INDEX_TOMBSTONE]).

gen_bucket() ->
    {<<"default">>, <<"b">>}.

gen_reason() ->
    oneof([put, delete, handoff]).

gen_solr_result() ->
    frequency([{75, {ok, "200", some, crap}},
               {5,  {ok, "400", bad, request}},
               {20, {error, reqd_timeout}}]).

gen_entries() ->
    non_empty(list({gen_partition(), gen_index(), gen_bucket(), gen_reason(), gen_solr_result()})).

gen_params() ->
    ?LET({HWMSeed, MinSeed, MaxSeed},
         {nat(), nat(), nat()},
         {1 + HWMSeed, 1 + MinSeed, 1 + MinSeed + MaxSeed}).


%%
%% Quickcheck Properties
%%

prop_ok() ->
    ?SETUP(
        fun() -> setup(), fun() -> cleanup() end end,
        ?FORALL(
            {Entries0, {HWM, Min, Max}},
            {gen_entries(), gen_params()},
            begin
                true = lists:member({'PULSE-REPLACE-MODULE',1},
                                           ?MODULE:module_info(exports)),
                true = lists:member({'PULSE-REPLACE-MODULE',1},
                                           yz_solrq:module_info(exports)),
                true = lists:member({'PULSE-REPLACE-MODULE',1},
                                           yz_solrq_helper:module_info(exports)),

                %% Reset the solrq/solrq helper processes
                application:set_env(yokozuna, solrq_queue_hwm, HWM),
                application:set_env(yokozuna, solrq_batch_min, Min),
                application:set_env(yokozuna, solrq_batch_max, Max),
                application:set_env(yokozuna, solrq_delayms_max, 10),
                application:set_env(yokozuna, purge_blown_indices, false),

                %% Prepare the entries, and set up the ibrowse mock
                %% to respond based on what was generated.
                Entries = add_keys(Entries0),
                KeyRes = make_keyres(Entries),
                PE = entries_by_vnode(Entries),

                meck:expect(
                    ibrowse, send_req,
                    fun(_Url, _H, _M, B, _O, _T) ->
                        %% TODO: Add check for index from URL
                        {Keys, Res} = yz_solrq_eqc_ibrowse:get_response(B),
                        solr_responses:record(Keys, Res),
                        Res
                    end
                ),

                ?PULSE(
                    {SolrQ, Helper, IBrowseKeys, MeltsByIndex},
                    begin
                        reset(), % restart the processes
                        unlink_kill(yz_solrq_0001),
                        unlink_kill(yz_solrq_helper_0001),
                        unlink_kill(yz_solrq_eqc_fuse),
                        unlink_kill(yz_solrq_eqc_ibrowse),
                        {ok, SolrQ} = yz_solrq:start_link(yz_solrq_0001),
                        {ok, Helper} = yz_solrq_helper:start_link(yz_solrq_helper_0001),
                        {ok, _} = yz_solrq_eqc_fuse:start_link(),
                        {ok, _} = yz_solrq_eqc_ibrowse:start_link(KeyRes),

                        %% Issue the requests under pulse
                        Pids = ?MODULE:send_entries(PE),
                        start_drains(length(Entries)),
                        wait_for_vnodes(Pids, timer:seconds(20)),
                        timer:sleep(500),
                        catch yz_solrq_eqc_ibrowse:wait(expected_keys(Entries)),
                        {SolrQ, Helper,  yz_solrq_eqc_ibrowse:keys(), melts_by_index(Entries)}
                    end,
                    ?WHENFAIL(
                        begin
                            eqc:format("SolrQ: ~p\n", [SolrQ]),
                            eqc:format("Helper: ~p\n", [Helper]),
                            eqc:format("KeyRes: ~p\n", [KeyRes]),
                            eqc:format("keys(): ~p\n", [IBrowseKeys]),
                            eqc:format("expected_entry_keys: ~p\n", [expected_entry_keys(PE)]),
                            eqc:format("PE: ~p\n", [PE]),
                            eqc:format("melts_by_index: ~p~n", [MeltsByIndex]),
                            eqc:format("Entries: ~p\n", [Entries]),
                            %debug_history([ibrowse, solr_responses, yz_kv])
                            debug_history([solr_responses])
                        end,
                        begin
                        %% For each vnode, spawn a process and start sending
                        %% Once all vnodes have sent, small delay to give async stuff time to catch up
                        %% Check all the objects that we expected were delivered.
                        %Expect = lists:sort(lists:flatten([Es || {_P,Es} <- PE])),
                        %equals(Expect, lists:sort(ibrowse_requests()))

                        HttpRespByKey = http_response_by_key(),
                        HashtreeHistory = hashtree_history(),
                        HashtreeExpect = hashtree_expect(Entries, HttpRespByKey),

                        %eqc:collect({hwm, HWM},
                        %    eqc:collect({batch_min, Min},
                        %        eqc:collect({batch_max, Max},
                                    conjunction([
                                        {solr, equals(
                                            lists:sort(IBrowseKeys),
                                            solr_expect(Entries))},
                                        {hashtree, equals(HashtreeHistory, HashtreeExpect)},
                                        {insert_order, ordered(expected_entry_keys(PE), IBrowseKeys)},
                                        {melts, equals(MeltsByIndex, errors_by_index(Entries))}
                                    ])
                        %        )
                        %    )
                        %)
                        end
                    )

                )
            end
        )
    ).

%%
%% Internal functions
%%

setup() ->
    %% Todo: Try trapping lager_msg:new/4 instead
    %% meck:new(lager, [passthrough]),
    %% meck:expect(lager, log, fun(_,_,Fmt,Args) ->
    %%                                 io:format(user, "LAGER: " ++ Fmt, Args)
    application:start(syntax_tools),
    application:start(compiler),
    application:start(goldrush),
    application:start(lager),

    yz_solrq_sup:set_solrq_tuple(1), % for yz_solrq_sup:regname
    yz_solrq_sup:set_solrq_helper_tuple(1), % for yz_solrq_helper_sup:regname

    meck:new(ibrowse),
    %% meck:expect(ibrowse, send_req, fun(_A, _B, _C, _D, _E, _F) ->
    %%                                     io:format("REQ: ~p\n", [{_A,_B,_C,_D,_E,_F}]),
    %%                                     {ok, "200", some, crap} end),

    meck:new(exometer),
    meck:expect(exometer, update, fun(_,_) -> ok end),

    meck:new(riak_kv_util),
    meck:expect(riak_kv_util, get_index_n, fun(BKey) -> {erlang:phash2(BKey) rem 16, 3} end),

    meck:new(riak_core_bucket),
    meck:expect(riak_core_bucket, get_bucket, fun get_bucket/1),

    meck:new(yz_misc, [passthrough]),
    meck:expect(yz_misc, get_ring, fun(_) -> fake_ring_from_yz_solrq_eqc end),
    meck:expect(yz_misc, owned_and_next_partitions, fun(_Node, _Ring) -> ordsets:new() end),
    meck:expect(yz_misc, filter_out_fallbacks, fun(_OwnedAndNext, Entries) -> Entries end),

    meck:new(yz_cover, [passthrough]),
    meck:expect(yz_cover, logical_index, fun(_) -> fake_logical_index_from_yz_solrq_eqc end),
    meck:expect(yz_cover, logical_partition, fun(_, _) -> 4321 end),

    meck:new(yz_extractor, [passthrough]),
    meck:expect(yz_extractor, get_def, fun(_,_) -> ?MODULE end), % dummy local module for extractor

    meck:new(yz_kv, [passthrough]),
    meck:expect(yz_kv, is_owner_or_future_owner, fun(_,_,_) -> true end),
    meck:expect(yz_kv, update_hashtree, fun(_Action, _Partition, _IdxN, _BKey) -> ok end),
    meck:expect(yz_kv, update_aae_exchange_stats, fun(_P, _TreeId, _Count) -> ok end),

    meck:new(fuse),
    meck:expect(fuse, ask, fun(IndexName, Context) -> yz_solrq_eqc_fuse:ask(IndexName, Context) end),
    meck:expect(fuse, melt, fun(IndexName) -> yz_solrq_eqc_fuse:melt(IndexName) end),

    %% Fake module to track solr responses - meck:history(solr_responses)
    meck:new(solr_responses, [non_strict]),
    meck:expect(solr_responses, record, fun(_Keys, _Response) -> ok end),

    %% Apply the pulse transform to the modules in the test
    %% Pulse compile solrq/solrq helper
    %% TODO dynamically pulse_instrument
%    Opts = [export_all,
%            {parse_transform,pulse_instrument},
%            {pulse_replace_module, [{gen_server, pulse_gen_server}]}],
%    yz_pulseh:compile(yz_solrq_eqc, Opts),
%    yz_pulseh:compile(yz_solrq, Opts),
%    yz_pulseh:compile(yz_solrq_helper, Opts),

    %% And start up supervisors to own the solrq/solrq helper
    %% {ok, SolrqSup} = yz_solrq_sup:start_link(1),
    %% {ok, HelperSup} = yz_solrq_helper_sup:start_link(1),
    %% io:format(user, "SolrqSup = ~p HelperSup = ~p\n", [SolrqSup, HelperSup]),
    ok.


cleanup() ->
    meck:unload(),
    %% unlink_kill(yz_solrq_helper_sup),
    %% unlink_kill(yz_solrq_sup),

    catch application:stop(fuse),

    catch application:stop(lager),
    catch application:stop(goldrush),
    catch application:stop(compiler),
    catch application:stop(syntax_tools),

    ok.

reset() ->
    %% restart(yz_solrq_sup, yz_solrq_0001),
    %% restart(yz_solrq_helper_sup, yz_solrq_helper_0001),
    meck:reset(ibrowse),
    meck:reset(solr_responses),
    meck:reset(yz_kv),
    ok.



%% Return the parsed solr history - which objects were dequeued and sent over HTTP
solr_history() ->
    lists:sort(dict:fetch_keys(http_response_by_key())).

solr_expect(Entries) ->
    lists:sort(
        [Key || {_P, Index, _Bucket, Key, _Op, Result} <- Entries,
            Index /= ?YZ_INDEX_TOMBSTONE,
            Result /= {ok, "400", bad, request}
        ]
    ).


%% Return the hashtree history
%% {<0.11796.2>,
%%  {yz_kv,update_hashtree,
%%         [{insert,<<131,98,5,19,185,115>>},
%%          0,
%%          {7,3},
%%          {{<<"default">>,<<"b">>},<<"k1">>}]},
%%  ok},
%% {<0.11759.2>,
%%  {yz_kv,update_hashtree,[delete,3,{8,3},{{<<"default">>,<<"b">>},<<"k2">>}]},
hashtree_history() ->
    Calls = [Args || {_Pid, {yz_kv, update_hashtree, Args}, ok} <- meck:history(yz_kv)],
    Updates = [{P, Key, case Op of {insert, _Hash} -> insert; _ -> Op end} ||
                  [Op, P, _Tree, {_Bucket,Key}] <- Calls],
    %% *STABLE* sort needed on P/BKey so that the order of operations on a key is correct
    lists:sort(Updates).

hashtree_expect(Entries, RespByKey) ->
    %% NB. foldr so no reverse, NB Result is per-entry result overridden because
    %% of batching.
    Expect = lists:foldr(fun({P, ?YZ_INDEX_TOMBSTONE, _Bucket, Key, delete, _Result}, Acc) ->
                                 [{P, Key, delete} | Acc];
                            ({P, ?YZ_INDEX_TOMBSTONE, _Bucket, Key, Op, _Result}, Acc) when Op == put;
                                                                                 Op == handoff ->
                                 [{P, Key, insert} | Acc];
                            ({_P, _Index, _Bucket, Key, _Op, _Result} = E, Acc) ->
                                 case get_http_response(Key, RespByKey) of
                                     {ok, "200", _, _} ->
                                         [hashtree_expect_entry(E) | Acc];
                                     {ok, "400", _, _} ->
                                         [hashtree_expect_entry(E) | Acc];
                                     _ ->
                                         Acc
                                 end
                         end, [], Entries),
    %% *STABLE* sort on P/BKey
    lists:sort(Expect).

hashtree_expect_entry({P, _Index, _Bucket, Key, delete, _Result}) ->
    %% If a successful delete, expect delete from AAE
    {P, Key, delete};
hashtree_expect_entry({P, _Index, _Bucket, Key, Op, _Result}) when Op == handoff;
                                                              Op == put ->

    {P, Key, insert}.


%% Expand to a dict of Key -> ibrowse:send_req returns
http_response_by_key() ->
    KeysResp = [{Keys, Resp} || {_Pid, {solr_responses, record,
                                        [Keys, Resp]}, ok} <- meck:history(solr_responses)],
    dict:from_list(lists:flatten([[{Key, Resp} || Key <- Keys] ||
                                     {Keys, Resp} <- KeysResp])).

%% Look up an http response by the sequence batch it was in
get_http_response(Key, RespByKey) ->
    dict:fetch(Key, RespByKey).


melts_by_index(Entries) ->
    Indices = lists:usort([Index || {_P, Index, _Bucket, _Key, _Op, _Result} <- Entries]),
    MeltsByIndex = [{Index, yz_solrq_eqc_fuse:melts(Index)} || Index <- Indices, Index /= ?YZ_INDEX_TOMBSTONE],
    %lager:info("FDUSHIN> MeltsByIndex: ~p", [MeltsByIndex]),
    [{Index, Melts} || {Index, Melts} <- MeltsByIndex, Melts /= 0].
    %MeltsByIndex.

errors_by_index(Entries) ->
    IndexErrors = [Index || {_P, Index, _Bucket, _Key, _Op, Result} <-
        Entries, Result == {error, reqd_timeout}, Index /= ?YZ_INDEX_TOMBSTONE],
    Partitions = partition(
        fun(I1, I2) -> I1 == I2 end,
        IndexErrors
    ),
    ErrorsByIndex = [{Index, length(Indices)} || [Index | _Rest] = Indices <- Partitions],
    sort_by_key(ErrorsByIndex).

sort_by_key(PropList) ->
    SortedKeys = lists:usort(proplists:get_keys(PropList)),
    [{Key, proplists:get_value(Key, PropList)} || Key <- SortedKeys].

%% The set of Keys that were written are ordered
%% by partition if for each set of keys [key_1, ..., key_n]
%% inserted under partion P for Index I, key_1, ..., key_n are ordered
%% in Keys.  I.e., for each key_i, key_j in {1..n}, if i < j,
%% then both key_i and key_j are in Keys, and key_i occurs
%% earlier in Keys than key_j.
%%
ordered(PartitionEntryKeys, Keys) ->
    lists:all(
        fun({_P, EntryKeys}) ->
            subseteq_ordered(EntryKeys, Keys)
        end,
        PartitionEntryKeys
    ).

subseteq_ordered(A, B) ->
    A == lists:filter(fun(Be) -> lists:member(Be, A) end, B).


%% PE =    [{partition(), [{index(), bucket(), key(), reason(), result()}]}]
%% returns [{{partition(), index()}, [key()]}]
%% where Index /= ?YZ_INDEX_TOMBSTONE and Reason /= {ok, "400", _, _}
expected_entry_keys(PE) ->
    KeysByIndexByPartition = lists:map(
        fun({P, Entries}) ->
            FilteredEntries = lists:filter(
                fun({Index, _Bucket, _Key, _Reason, Result}) ->
                    case Result of
                        {ok, "400", _Bad, _Request} ->
                            false;
                        _ ->
                            Index /= ?YZ_INDEX_TOMBSTONE
                    end
                end,
                Entries
            ),
            PartitionedEntries = partition(
                fun({Index1, _, _, _, _}, {Index2, _, _, _, _}) -> Index1 == Index2 end,
                FilteredEntries
            ),
            KeysByIndex = lists:map(
                fun([{Index, _, _, _, _}|_T] = NonEmptyListOfEntriesWithSameIndex) ->
                    Keys = [Key || {I, _, Key, _, _} <- NonEmptyListOfEntriesWithSameIndex, I == Index],
                    {Index, Keys}
                end,
                PartitionedEntries
            ),
            {P, KeysByIndex}
        end,
        orddict:to_list(PE)
    ),
    [{{P, Index}, Keys} || {P, KeysByIndex} <- KeysByIndexByPartition, {Index, Keys} <- KeysByIndex].

%% partition a list based on an equivalence relation, R.
%% R must be reflexive, symmetic, and transitive over elements in L.
%% returns a list L of lists, such that for each
%% L' = [E_1, ..., E_n] in L, R(E_i, E_j), for each i, j in {1..n},
%% Union(L') = L and intersection(L') = []
partition(R, L) ->
    lists:foldl(
        fun(E, Accum) ->
            {L1, L2} = lists:partition(
                fun([E_i|_T]) ->
                    R(E_i, E)
                end,
                Accum
            ),
            case L1 of
                []  -> [[E]      | L2];
                [H] -> [H ++ [E] | L2]
            end
        end,
        [],
        L
    ).


%% Mocked extractor
extract(Value) ->
    [{yz_solrq_eqc, Value}].

    %% %% THE MOCKED FALLBACK
%% If bt_no_index or bn_no_index
get_bucket({<<"bt_no_index">>,_BN}) ->
    [{n_val, 3}];
get_bucket({_BT,<<"bn_no_index">>}) ->
    [{n_val, 3}];
get_bucket({_BT,_BN}=_B) ->
    [{search_index, <<"index1">>}, {n_val, 3}].

restart(Sup, Id) ->
    catch supervisor:terminate_child(Sup, Id),
    {ok, _} = supervisor:restart_child(Sup, Id).

unlink_kill(Name) ->
    try
        Pid = whereis(Name),
        unlink(Pid),
        exit(Pid, kill)
    catch _:_ ->
            true
    end.


add_keys(Entries) ->
    [{P, Index, Bucket, make_key(Seq), Reason, Result} ||
        {Seq, {P, Index, Bucket, Reason, Result}} <- lists:zip(lists:seq(1, length(Entries)),
                                                               Entries)].
make_key(Seq) ->
    list_to_binary(["XKEYX"++integer_to_list(Seq)]).

make_keyres(Entries) ->
    [{Key, Result} || {_P, _Index, _Bucket, Key, _Reason, Result} <- Entries].

expected_keys(Entries) ->
    [Key || {_P, Index, _Bucket, Key, _Reason, Result} <- Entries, Index /= ?YZ_INDEX_TOMBSTONE, Result /= {ok, "400", bad, request}].

    entries_by_vnode(Entries) ->
    lists:foldl(fun({P, Index, Bucket, Key, Reason, Result}, Acc) ->
                        orddict:append_list(P, [{Index, Bucket, Key, Reason, Result}], Acc)
                end, orddict:new(), Entries).

send_entries(PE) ->
    Self = self(),
    [spawn_link(fun() -> send_vnode_entries(Self, P, E) end) || {P, E} <- PE].

%% Send the entries for a vnode
send_vnode_entries(Runner, P, Events)  ->
    self() ! {ohai, length(Events)},
    [yz_solrq:index(Index, {Bucket, Key}, make_obj(Bucket, Key), Reason, P) || {Index, Bucket, Key, Reason, _Result} <- Events],
    receive
        {ohai, _Len} ->
            ok
    end,
    Runner ! {self(), done}.

make_obj(B,K) ->
    riak_object:new(B, K, K, "application/yz_solrq_eqc"). % Set Key as value



start_drains(_N) ->
    spawn_link(fun() -> drain(500) end).

drain(Millis) ->
    %ok = yz_solrq_sup:drain(),
    try
        {ok, Pid} = yz_solrq_drain_fsm:start_link(),
        Reference = erlang:monitor(process, Pid),
        yz_solrq_drain_fsm:start_prepare(),
        receive
            {'DOWN', Reference, _Type, _Object, normal} ->
                ok;
            {'DOWN', Reference, _Type, _Object, Info} ->
                lager:info("FDUSHIN> Info: ~p", [Info]),
                {error, Info}
        after 10000 ->
            erlang:demonitor(Reference),
            %yz_solrq_drain_fsm:maybe_cancel()
            {error, timeout}
        end
    catch
        _:badarg ->
            {error, in_progress}
    end,
    timer:sleep(Millis),
    drain(Millis).

%% Wait for send_entries - should probably set a global timeout and
%% and look for that instead
wait_for_vnodes(Pids, _Timeout) ->
    RRef = make_ref(),
    %% TRef = erlang:send_after(Timeout, self(), {timeout, RRef}),
    wait_for_vnodes_msgs(Pids, RRef),
    %% erlang:cancel_timer(TRef),
    receive
        {timeout, _TRef} -> %todo - remove underscore if renable timeout
            ok
    after
        0 ->
            ok
    end.

wait_for_vnodes_msgs([], _Ref) ->
    ok;
wait_for_vnodes_msgs([Pid | Pids], Ref) ->
    receive
        {Pid, done} ->
            wait_for_vnodes_msgs(Pids, Ref);
        {timeout, Ref} ->
            throw(timeout);
        {timeout, OldRef} ->
            io:format(user, "Ignoring old timer ref ~p\n", [OldRef]),
            wait_for_vnodes_msgs([Pid|Pids], Ref)
    end.

%% ibrowse_requests() ->
%%     [ibrowse_call_extract(Args, Res) || {_Pid, {ibrowse, send_req, Args, Res}} <- meck:history(ibrowse)].

%% ibrowse_call_extract(Url, Header, post, JsonIolist, _Options, _Timeout) ->
%%     {parse_solr_url(Url), parse_solr_reqs(mochijson2:decode(JsonIolist))}.

debug_history(Mods) ->
    [io:format("~p\n====\n~p\n\n", [Mod, meck:history(Mod)]) || Mod <- Mods],
    ok.

-else. %% PULSE is not defined

pulse_warning_test() ->
    ?debugMsg("WARNING: PULSE is not defined.  Run `make pulse` to execute this test."),
    ok.

-endif. % PULSE

-else. %% EQC is not defined

-include_lib("eunit/include/eunit.hrl").
eqc_warning_test() ->
    ?debugMsg("WARNING: EQC is not defined.  Make sure EQC is installed and licensed with your current Erlang runtime."),
    ok.

-endif. % EQC
