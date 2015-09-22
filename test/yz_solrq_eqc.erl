%% Property
%% If SOLR accepted batch, all should be added to AAE
%% If SOLR failed batch, none should be added.

-module(yz_solrq_eqc).
-include_lib("eqc/include/eqc.hrl").
-include_lib("pulse/include/pulse.hrl").
-compile(export_all).
-compile({parse_transform,pulse_instrument}).
-include_lib("pulse_otp/include/pulse_otp.hrl").


run() ->
    run(10).

run(Secs) ->
    eqc:quickcheck(eqc:testing_time(Secs, prop_ok())).

check() ->
    eqc:check(prop_ok()).

recheck() ->
    eqc:recheck(prop_ok()).

gen_partition() ->
    nat().

gen_index() ->
    oneof([<<"idx1">>,<<"idx2">>]).

gen_bucket() ->
    {<<"default">>,<<"b">>}.

gen_reason() ->
    oneof([put, delete, handoff]).

gen_solr_result() ->
    frequency([{8, {ok, "200", some, crap}},
               {1, {ok, "500", some, crap}},
               {1, {error, reqd_timeout}}]).

gen_entries() ->
    non_empty(list({gen_partition(), gen_index(), gen_bucket(), gen_reason(), gen_solr_result()})).

gen_params() ->
    ?LET({HWMSeed, MinSeed, MaxSeed},
         {nat(), nat(), nat()},
         [1 + HWMSeed, 1 + MinSeed, 1 + MinSeed + MaxSeed]).

prop_ok() ->
    ?SETUP(fun() -> setup(), fun() -> cleanup() end end,
           ?FORALL({Entries0, {HWM, Min, Max}},
                   {gen_entries(), {10, 2, 2}}, %gen_params()},
                   begin
                       %% Reset the solrq/solrq helper processes
                       application:set_env(yokozuna, solrq_queue_hwm, HWM),
                       application:set_env(yokozuna, solrq_batch_min, Min),
                       application:set_env(yokozuna, solrq_batch_max, Max),
                       application:set_env(yokozuna, solrq_delayms_max, 10),

                       %% Prepare the entries, and set up the ibrowse mock
                       %% to respond based on what was generated.
                       Entries = add_keys(Entries0),
                       KeyRes = make_keyres(Entries),
                       PE = entries_by_vnode(Entries),


                       meck:expect(ibrowse, send_req,
                                   fun(_Url, _H, _M, B, _O, _T) ->
                                           %% TODO: Add check for index from URL
                                           SolrReq = parse_solr_reqs(mochijson2:decode(B)),
                                           {Keys, Res} = update_response(SolrReq, KeyRes),
                                           solr_responses:record(Keys, Res),
                                           Res
                                   end),

                       ?WHENFAIL(
                          begin
                              eqc:format("self = ~p  yz_solrq_0001 = ~p  yz_solrq_helper_0001 = ~p\n",
                                         [self(), whereis(yz_solrq_0001), whereis(yz_solrq_helper_0001)]),
                              eqc:format("Partition/Entries\n~p\n", [PE]),
                              debug_history([ibrowse, solr_responses, yz_kv])
                          end,
                          ?PULSE(
                          _Res,
                          begin
                              reset(), % restart the processes
                              unlink_kill(yz_solrq_0001),
                              unlink_kill(yz_solrq_helper_0001),
                              {ok,_SolrQ} = yz_solrq:start_link(yz_solrq_0001),
                              {ok,_Helper} = yz_solrq_helper:start_link(yz_solrq_helper_0001),

                              %% Issue the requests under pulse
                              Pids = send_entries(PE),
                              wait_for_vnodes(Pids, timer:seconds(20)),
                              timer:sleep(500)
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
                              ?WHENFAIL(begin
                                            eqc:format("Partition Entries\n=======\n~p\n\n", [PE])
                                        end,
                                        equals(HashtreeHistory, HashtreeExpect))
                          end))
                   end)).


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
    Expect = lists:foldr(fun({_P, _Index, _Bucket, Key, _Op, _Result} = E, Acc) ->
                                 case get_http_response(Key, RespByKey) of
                                     {ok, "200", _, _} ->
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


setup() ->
    %% Todo: Try trapping lager_msg:new/4 instead
    %% meck:new(lager, [passthrough]),
    %% meck:expect(lager, log, fun(_,_,Fmt,Args) ->
    %%                                 io:format(user, "LAGER: " ++ Fmt, Args)
    application:start(syntax_tools),
    application:start(compiler),
    application:start(goldrush),
    application:start(lager),

    application:start(fuse),

    meck:new(ibrowse),
    %% meck:expect(ibrowse, send_req, fun(_A, _B, _C, _D, _E, _F) -> 
    %%                                     io:format("REQ: ~p\n", [{_A,_B,_C,_D,_E,_F}]),
    %%                                     {ok, "200", some, crap} end),

    meck:new(exometer),
    meck:expect(exometer, update, fun(_,_) -> ok end),
    %% pulseh:compile(exometer),

    meck:new(riak_kv_util),
    meck:expect(riak_kv_util, get_index_n, fun(BKey) -> {erlang:phash2(BKey) rem 16, 3} end),
    %% pulseh:compile(exometer),

    meck:new(riak_core_bucket),
    meck:expect(riak_core_bucket, get_bucket, fun get_bucket/1),
    %% pulseh:compile(riak_core_bucket),

    meck:new(yz_misc, [passthrough]),
    meck:expect(yz_misc, get_ring, fun(_) -> fake_ring_from_yz_solrq_eqc end),
    %% pulseh:compile(yz_misc),

    meck:new(yz_cover, [passthrough]),
    meck:expect(yz_cover, logical_index, fun(_) -> fake_logical_index_from_yz_solrq_eqc end),
    meck:expect(yz_cover, logical_partition, fun(_, _) -> 4321 end),
    %% pulseh:compile(yz_cover),

    meck:new(yz_extractor, [passthrough]),
    meck:expect(yz_extractor, get_def, fun(_,_) -> ?MODULE end), % dummy local module for extractor
    %% pulseh:compile(yz_extractor),

    meck:new(yz_kv, [passthrough]),
    meck:expect(yz_kv, is_owner_or_future_owner, fun(_,_,_) -> true end),
    meck:expect(yz_kv, update_hashtree, fun(_Action, _Partition, _IdxN, _BKey) -> ok end),
    meck:expect(yz_kv, update_aae_exchange_stats, fun(_P, _TreeId, _Count) -> ok end),
    %% pulseh:compile(yz_kv),

    %% Fake module to track solr responses - meck:history(solr_responses)
    meck:new(solr_responses, [non_strict]),
    meck:expect(solr_responses, record, fun(_Keys, _Response) -> ok end),
    %% pulseh:compile(solr_responses),

%% TODO: Make dynamic
    %% Pulse compile solrq/solrq helper
    %% pulseh:compile(yz_solrq),
    %% pulseh:compile(yz_solrq_helper),

    %% And start up supervisors to own the solrq/solrq helper
    %% {ok, SolrqSup} = yz_solrq_sup:start_link(1),
    %% {ok, HelperSup} = yz_solrq_helper_sup:start_link(1),
    %% io:format(user, "SolrqSup = ~p HelperSup = ~p\n", [SolrqSup, HelperSup]),
    ok.


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

send_entries(PE) ->
    Self = self(),
    [spawn_link(fun() -> send_vnode_entries(Self, P, E) end) || {P, E} <- PE].

entries_by_vnode(Entries) ->
    lists:foldl(fun({P, Index, Bucket, Key, Reason, Result}, Acc) ->
                        orddict:append_list(P, [{Index, Bucket, Key, Reason, Result}], Acc)
                end, orddict:new(), Entries).

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

%% ibrowse_requests() ->
%%     [ibrowse_call_extract(Args, Res) || {_Pid, {ibrowse, send_req, Args, Res}} <- meck:history(ibrowse)].

%% ibrowse_call_extract(Url, Header, post, JsonIolist, _Options, _Timeout) ->
%%     {parse_solr_url(Url), parse_solr_reqs(mochijson2:decode(JsonIolist))}.

debug_history(Mods) ->
    [io:format("~p\n====\n~p\n\n", [Mod, meck:history(Mod)]) || Mod <- Mods],
    ok.


%% Returns [{add, Key}, {delete, Key}]
parse_solr_reqs({struct, Reqs}) ->
    [parse_solr_req(Req) || Req <- Reqs].

parse_solr_req({<<"add">>, {struct, [{<<"doc">>, Doc}]}}) ->
    {add, find_key_field(Doc)};
parse_solr_req({<<"delete">>, {struct, [{<<"query">>, Query}]}}) ->
    {delete, parse_delete_query(Query)};
parse_solr_req({delete, _Query}) ->
    {delete, could_parse_bkey};
parse_solr_req({Other, Thing}) ->
    {Other, Thing}.

parse_delete_query(Query) ->
    {match, [Key]} = re:run(Query, "(XKEYX[0-9]+)",[{capture,[1],binary}]),
    Key.

find_key_field({struct, Props}) ->
    proplists:get_value(<<"yz_solrq_eqc">>, Props).


%% Decide what to return for the request... If any of the seq
%% numbers had failures generated, apply to all of them.
update_response(SolrReqs, KeyRes) ->
    Keys = lists:usort([Key || {_Op, Key} <- SolrReqs]),
    Responses = [Res || {Key, Res} <- KeyRes, lists:member(Key, Keys)],
    {Keys, lists:foldl(fun update_response_folder/2, undefined, Responses)}.

update_response_folder(_, {error, _Err}=R) ->
    R;
update_response_folder(_, {ok, "500", _Some, _Crap}=R) ->
    R;
update_response_folder(R, _Acc) ->
    R.
