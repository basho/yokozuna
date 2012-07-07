-module(yz_solr_proc).
-include("yokozuna.hrl").
-compile(export_all).
-behavior(gen_server).

%% Keep compiler warnings away
-export([init/1,
         handle_info/2,
         terminate/2]).

-record(state, {
          dir=exit(dir_undefined),
          port=exit(port_undefined),
          solr_port=exit(solr_port_undefined)
         }).

-define(SHUTDOWN_MSG, "INT\n").
-define(S_MATCH, #state{dir=_Dir, port=_Port, solr_port=_SolrPort}).
-define(S_PORT(S), S#state.port).

%% @doc This module/process is responsible for administrating the
%%      external Solr/JVM OS process.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(string(), string()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Dir, SolrPort) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Dir, SolrPort], []).


%%%===================================================================
%%% Callbacks
%%%===================================================================

%% NOTE: Doing the work here will slow down startup but I think that's
%%       desirable given that Solr must be up for Yokozuna to work
%%       properly.
init([Dir, SolrPort]) ->
    process_flag(trap_exit, true),
    Cmd = build_cmd(SolrPort),
    ?DEBUG("running OS cmd ~p", [Cmd]),
    Port = run_cmd(Cmd, Dir),
    S = #state{
      dir=Dir,
      port=Port,
      solr_port=SolrPort
     },
    {ok, S}.

handle_info({_Port, {data, Data}}, S=?S_MATCH) ->
    ?DEBUG("~p", Data),
    {noreply, S};

handle_info({exit_status, ExitStatus}, S) ->
    exit({"solr OS process exited", ExitStatus, S}).

terminate(_, S) ->
    Port = ?S_PORT(S),
    port_command(Port, ?SHUTDOWN_MSG),
    port_close(Port),
    ok.


%%%===================================================================
%%% Private
%%%===================================================================

build_cmd(SolrPort) ->
    Wrapper = ?YZ_PRIV ++ "/sig-wrapper",
    Java = "java",
    HomeArg = "-Dsolr.solr.home=.",
    PortArg = "-Djetty.port=" ++ SolrPort,
    JarArg = "-jar start.jar",
    string:join([Wrapper, Java, HomeArg, PortArg, JarArg], " ").

run_cmd(Cmd, Dir) ->
    open_port({spawn, Cmd}, [{cd, Dir}, exit_status]).
