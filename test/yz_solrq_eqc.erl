-module(yz_solrq_eqc).
-include_lib("eqc/include/eqc.hrl").
-compile([export_all]).

run() ->
    eqc:quickcheck(prop_ok()).

gen_vnode() ->
    oneof([va, vb, vc, vd, ve]).

gen_index() ->
    oneof([idx1,idx2]).

gen_bkey() ->
    oneof([k1, k2, k3]).

gen_solr_result() ->
    oneof([200, 500, timeout]).

gen_entries() ->
    non_empty(list({gen_vnode(), gen_index(), gen_bkey(), gen_solr_result()})).

prop_ok() ->
    ?FORALL(Entries,
            gen_entries(),
            begin
                %% Restart subsystem
                meck:new(ibrowse, [non_strict]),
                meck:expect(ibrowse, send_req, fun({_V, _I, _B, R}) -> R end),
                try
                    Pids = send_entries(Entries),
                    wait_for_vnodes(Pids, timer:seconds(20)),

                    %% For each vnode, spawn a process and start sending
                    %% Once all vnodes have sent, small delay to give async stuff time to catch up
                    %% Check all the objects that we expected were delivered.
                    equals(lists:sort(Entries), lists:sort(ibrowse_requests()))
                after
                    meck:unload(ibrowse)
                end
            end).



send_entries(Entries) -> % Vnode, Index, BKey, Result
    VE = entries_by_vnode(Entries),
    Self = self(),
    [spawn_link(fun() -> send_vnode_entries(Self, E) end) || {_V, E} <- VE].

entries_by_vnode(Entries) ->
    lists:foldl(fun({V,_I,_B,_R}=E, Acc) ->
                        orddict:append_list(V, [E], Acc)
                end, orddict:new(), Entries).

%% Wait for send_entries - should probably set a global timeout and
%% and look for that instead
wait_for_vnodes(Pids, Timeout) ->
    RRef = make_ref(),
    TRef = erlang:send_after(Timeout, self(), {timeout, RRef}),
    wait_for_vnodes_msgs(Pids, RRef),
    erlang:cancel_timer(TRef),
    receive
        {timeout, TRef} ->
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

send_vnode_entries(Runner, Events)  ->
    [ibrowse:send_req(E) || E <- Events],
    Runner ! {self(), done}.

%% Dummy
index(E) ->
    ibrowse:send_req(E).

ibrowse_requests() ->
    [E || {_Pid, {ibrowse, send_req, [E]}, _Res} <- meck:history(ibrowse)].
