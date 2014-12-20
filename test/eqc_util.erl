-module(eqc_util).
-compile(export_all).

-include("yokozuna.hrl").

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").

%%%===================================================================
%%% Generators
%%%===================================================================

vclock() ->
    ?LET(VclockSym, vclock_sym(), eval(VclockSym)).

vclock_sym() ->
    ?LAZY(
       oneof([
              {call, vclock, fresh, []},
              ?LETSHRINK([Clock], [vclock_sym()],
                         {call, ?MODULE, increment,
                          [noshrink(binary(4)), nat(), Clock]})
              ])).

increment(Actor, Count, Vclock) ->
    lists:foldl(
      fun vclock:increment/2,
      Vclock,
      lists:duplicate(Count, Actor)).

riak_object() ->
    ?LET({{Bucket, Key}, Vclock, Value},
         {bkey(), vclock(), binary()},
         riak_object:set_vclock(riak_object:new(Bucket, Key, Value),
                                Vclock)).

not_empty(G) ->
    ?SUCHTHAT(X, G, X /= [] andalso X /= <<>>).

bkey() ->
    {non_blank_string(),  %% bucket
     non_blank_string()}. %% key

non_blank_string() ->
    ?LET(X,not_empty(list(lower_char())), list_to_binary(X)).

%% Generate a lower 7-bit ACSII character that should not cause any problems
%% with utf8 conversion.
lower_char() ->
    choose(16#20, 16#7f).

%%%===================================================================
%%% Mocks, Env-Sets
%%%===================================================================

set_core_envs() ->
    application:set_env(riak_core, default_bucket_props,
                        [{n_val, 1},
                         {search_index, "test"},
                         {chash_keyfun,{riak_core_util,chash_std_keyfun}}]),
    application:set_env(riak_core, ring_state_dir, "ring"),
    application:set_env(riak_core, platform_data_dir, "eqc_test_data"),
    application:set_env(riak_core, handoff_port, 0).

set_kv_envs() ->
    application:set_env(riak_kv, anti_entropy_data_dir, "kv_aae"),
    application:set_env(riak_kv, anti_entropy, {on, []}).

set_yokozuna_envs() ->
    application:set_env(yokozuna, anti_entropy_data_dir, "yz_aae"),
    application:set_env(yokozuna, enabled, true).

start_mock_components() ->
    application:load(riak_core),
    application:load(riak_kv),
    application:load(lager),
    ok = lager:start(),
    riak_core_metadata_manager:start_link([{data_dir, "eqc_test_data"}]),
    riak_core_ring_events:start_link(),
    riak_core_ring_manager:start_link(test),
    riak_core_ring_manager:setup_ets(test),
    setup_mockring(),
    riak_kv_entropy_info:create_table(),
    riak_kv_entropy_manager:start_link(),
    ok.

cleanup_mock_components() ->
    riak_core_ring_manager:cleanup_ets(test),
    application:stop(lager),
    application:unload(lager),
    stop_process(riak_core_ring_manager),
    stop_process(riak_core_metadata_manager),
    stop_process(riak_kv_entropy_manager),
    application:stop(riak_core),
    application:stop(riak_kv),
    application:unload(riak_core),
    application:unload(riak_kv),
    ok.

%% Stop a running pid - unlink and exit(kill) the process
stop_process(undefined) ->
    ok;
stop_process(RegName) when is_atom(RegName) ->
    stop_process(whereis(RegName));
stop_process(Pid) when is_pid(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    ok = wait_for_pid(Pid).

%% Wait for a pid to exit
wait_for_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),
    receive
        {'DOWN',Mref,process,_,_} ->
            ok
    after
        5000 ->
            {error, didnotexit, Pid, erlang:process_info(Pid)}
    end.

setup_mockring() ->
    % requires a running riak_core_ring_manager, in test-mode is ok
    Ring0 = riak_core_ring:fresh(16, node()),
    Ring1 = riak_core_ring:add_member(node(), Ring0, 'othernode@otherhost'),
    Ring2 = riak_core_ring:add_member(node(), Ring1, 'othernode2@otherhost2'),

    Ring3 = lists:foldl(fun(_,R) ->
                               riak_core_ring:transfer_node(
                                 hd(riak_core_ring:my_indices(R)),
                                 'othernode@otherhost', R) end,
                        Ring2,[1,2,3,4,5,6]),
    Ring = lists:foldl(fun(_,R) ->
                               riak_core_ring:transfer_node(
                                 hd(riak_core_ring:my_indices(R)),
                                 'othernode2@otherhost2', R) end,
                       Ring3,[1,2,3,4,5,6]),
    riak_core_ring_manager:set_ring_global(Ring).

-spec get_bkey_from_object(obj()) -> {bkey(), bkey()}.
get_bkey_from_object(RObj) ->
    {riak_object:bucket(RObj), riak_object:key(RObj)}.

-endif. % EQC
-endif. % Test
