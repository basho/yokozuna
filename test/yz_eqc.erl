-module(yz_eqc).

-compile(export_all).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").


-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

% -behaviour(eqc_statem).

-record(state, {server_pid,
                cores=[]}).

prop_yz_test_() ->
    {spawn, 
     [{setup,
       fun setup/0,
       fun cleanup/1,
       [%% Check networking/clients are set up 
        ?_assert(node() /= 'nonode@nohost'),
        ?_assertEqual(pong, net_adm:ping(node())),
        {timeout, 60, [?_assertEqual(pang, net_adm:ping('nonode@nohost'))]},
        ?_assertMatch({ok,_C}, riak:local_client()),
        %% Run the quickcheck tests
        {timeout, 60000, % do not trust the docs - timeout is in msec
          ?_assertEqual(true, quickcheck(numtests(500, ?QC_OUT(prop_solr()))))
        }
       ]
      }
     ]
    }.

setup() ->
  build_yz_dir(),
  State = case net_kernel:stop() of
            {error, not_allowed} ->
                running;
            _ ->
                %% Make sure epmd is started - will not be if erl -name has
                %% not been run from the commandline.
                os:cmd("epmd -daemon"),
                timer:sleep(100),
                TestNode = list_to_atom("yzeqc" ++ integer_to_list(element(3, now())) ++ integer_to_list(element(2, now()))),
                {ok, _Pid} = net_kernel:start([TestNode, shortnames]),
                started
        end,
  ok = application:load(yokozuna),
  ok = application:start(ibrowse),
  ok = application:start(lager),
  application:set_env(sasl, sasl_error_logger, {file, "get_fsm_eqc_sasl.log"}),
  ok = application:start(sasl),
  ok = application:start(riak_sysmon),
  ok = application:start(inets),
  ok = application:start(mochiweb),
  ok = application:start(webmachine),
  ok = application:start(os_mon),
  application:set_env(riak_core, default_bucket_props,
                      [{r, quorum}, {w, quorum}, {pr, 0}, {pw, 0},
                       {rw, quorum}, {n_val, 3}]),
  application:set_env(riak_kv, storage_backend, riak_kv_memory_backend),
  ok = application:load(riak_core),
  ok = application:load(riak_kv),

  Vars = [{ring_creation_size, 8},
          {ring_state_dir, "<nostore>"},
          {cluster_name, "test"},
          %% Don't allow rolling start of vnodes as it will cause a
          %% race condition with `all_nodes'.
          {vnode_rolling_start, 0}],
  OldVars = [begin
                 Old = app_helper:get_env(riak_core, AppKey),
                 ok = application:set_env(riak_core, AppKey, Val),
                 {AppKey, Old}
             end || {AppKey, Val} <- Vars],
  ok = application:start(riak_core),
  ok = application:start(riak_api),
  ok = application:start(erlang_js),
  ok = application:start(riak_pipe),

  ok = application:start(riak_kv),

  application:stop(yokozuna),
  State.

stop_servers() ->
    %% Make sure VMaster is killed before sup as start_vnode is a cast
    %% and there may be a pending request to start the vnode.
    stop_pid(whereis(mock_vnode_master)),
    stop_pid(whereis(riak_core_vnode_manager)),
    stop_pid(whereis(riak_core_vnode_sup)).

build_yz_dir() ->
  yz_misc:make_dirs(["data"]),
  yz_misc:copy_files(["../priv/solr"], "data"),
  file:rename("data/solr", "data/yz").

stop_pid(undefined) ->
    ok;
stop_pid(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    ok = wait_for_pid(Pid).

wait_for_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),
    receive
        {'DOWN',Mref,process,_,_} ->
            ok
    after
        5000 ->
            {error, didnotexit}
    end.


cleanup(running) ->
  application:stop(yokozuna),
  % application:stop(lager),
  % application:unload(lager),
  % cleanup_javascript(),
  % fsm_eqc_util:cleanup_mock_servers(),
  % %% Cleanup the JS manager process
  % %% since it tends to hang around too long.
  % case whereis(?JSPOOL_HOOK) of
  %     undefined ->
  %         ignore;
  %     Pid ->
  %         exit(Pid, put_fsm_eqc_js_cleanup)
  % end,
  ok;
cleanup(started) ->
  ok = net_kernel:stop(),
  cleanup(running).

initial_state() -> 
  #state{cores=[]}.

-define(core, elements(["a","b","c","d"])).

command(_S) ->
  oneof(
    % [{call,?MODULE,index_create,[?core]}] ++
    [{call,?MODULE,solr_ping,[?core]}] ++
    [{call,?MODULE,solr_commit,[?core]}] ++
    % [{call,?MODULE,solr_ping,[?core]} || S#state.cores /= []] ++
    % [{call,?MODULE,solr_commit,[?core]} || S#state.cores /= []]
    []
  ).

next_state(S,_V,{call,_,index_create,[Core]}) ->
  S#state{cores = [Core|S#state.cores]};
next_state(S,_V,{call,_,solr_ping,[_Core]}) ->
  S;
next_state(S,_V,{call,_,solr_commit,[_Core]}) ->
  S.


index_create(Core) ->
  ok = yz_index:create(Core),
  % TODO: block until exists
  ok.

solr_commit(Core) ->
  yz_solr:commit(Core).

solr_ping(Core) ->
  yz_solr:ping(Core).


precondition(S,{call,_,solr_ping,[Core]}) ->
  % not lists:keymember(Core, 1, S#state.cores);
  lists:member(Core, S#state.cores);
precondition(S,{call,_,solr_commit,[Core]}) ->
  lists:member(Core, S#state.cores);
precondition(_S,{call,_,_,_}) ->
  true.

% postcondition(S,{call,_,solr_ping,_},R) -> 
%   true;
postcondition(_S,{call,_,_,_},_R) ->
  true.


prop_solr() ->
  ok = application:start(yokozuna),

  % true.
  % ok = application:start(yokozuna),

  % % ?FORALL(Cmds,fault_rate(1,10,commands(?MODULE)),
  ?FORALL(Cmds, commands(?MODULE),
    ?TRAPEXIT(
    begin
        {H,S,Res} = run_commands(?MODULE,Cmds),
        % cleanup
        % application:stop(yokozuna),
        pretty_commands(?MODULE,Cmds,{H,S,Res}, Res == ok)
    end)).

-endif.
