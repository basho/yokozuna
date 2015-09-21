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
                    [index(E) || E <- Entries],

                    %% For each vnode, spawn a process and start sending
                    %% Once all vnodes have sent, small delay to give async stuff time to catch up
                    %% Check all the objects that we expected were delivered.
                    equals(Entries, ibrowse_requests())
                after
                    meck:unload(ibrowse)
                end
            end).


%% Dummy
index(E) ->
    ibrowse:send_req(E).

ibrowse_requests() ->
    [E || {_Pid, {ibrowse, send_req, [E]}, _Res} <- meck:history(ibrowse)].
