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

-module(yz_solr_proc).
-include("yokozuna.hrl").
-compile(export_all).
-behavior(gen_server).

%% Keep compiler warnings away
-export([code_change/3,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         getpid/0]).

-record(state, {
          dir=exit(dir_undefined),
          port=exit(port_undefined),
          solr_port=exit(solr_port_undefined),
          solr_jmx_port=exit(solr_jmx_port_undefined)
         }).

-define(SHUTDOWN_MSG, "INT\n").
-define(S_MATCH, #state{dir=_Dir,
                        port=_Port,
                        solr_port=_SolrPort,
                        solr_jmx_port=_SolrJMXPort}).
-define(S_PORT(S), S#state.port).

%% @doc This module/process is responsible for administrating the
%%      external Solr/JVM OS process.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(string(), string(), string()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Dir, SolrPort, SolrJMXPort) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Dir, SolrPort, SolrJMXPort], []).

%% @doc Get the operating system's PID of the Solr/JVM process.  May
%%      return `undefined' if Solr failed to start.
-spec getpid() -> undefined | pos_integer().
getpid() ->
    gen_server:call(?MODULE, getpid).

%%%===================================================================
%%% Callbacks
%%%===================================================================

%% NOTE: JVM startup is broken into two pieces:
%%       1. ensuring the data dir exists, and spawning the JVM process
%%       2. checking whether solr finished initialization
%%
%%       The first bit is done here, because if it take more than the
%%       five second supervisor limit, something is *really* wrong.
%%
%%       The second bit is done once per second until solr is found to
%%       be up, or until the startup timeout expires. The idea behind
%%       the once-per second check is to get something in the log soon
%%       after Solr is confirmed up, instead of waiting until the
%%       timeout expires.
init([Dir, SolrPort, SolrJMXPort]) ->
    ensure_data_dir(Dir),
    process_flag(trap_exit, true),
    {Cmd, Args} = build_cmd(SolrPort, SolrJMXPort, Dir),
    ?INFO("Starting solr: ~p ~p", [Cmd, Args]),
    Port = run_cmd(Cmd, Args),
    schedule_solr_check(solr_startup_wait()),
    S = #state{
      dir=Dir,
      port=Port,
      solr_port=SolrPort,
      solr_jmx_port=SolrJMXPort
     },
    {ok, S}.

handle_call(getpid, _, S) ->
    {reply, get_pid(?S_PORT(S)), S};
handle_call(Req, _, S) ->
    ?WARN("unexpected request ~p", [Req]),
    {noreply, S}.

handle_cast(Req, S) ->
    ?WARN("unexpected request ~p", [Req]),
    {noreply, S}.

handle_info({check_solr, WaitTimeSecs}, S=?S_MATCH) ->
    case is_up() of
        true ->
            %% solr finished its startup, be merry
            ?INFO("solr is up", []),
            riak_core_node_watcher:service_up(yokozuna, self()),
            {noreply, S};
        false when WaitTimeSecs > 0 ->
            %% solr hasn't finished yet, keep waiting
            schedule_solr_check(WaitTimeSecs),
            {noreply, S};
        false ->
            %% solr did not finish startup quickly enough, or has just
            %% crashed and the exit message is on its way, shutdown
            {stop, "solr didn't start in alloted time", S}
    end;
handle_info({_Port, {data, Data}}, S=?S_MATCH) ->
    ?INFO("solr stdout/err: ~s", [Data]),
    {noreply, S};
handle_info({_Port, {exit_status, ExitStatus}}, S) ->
    {stop, {"solr OS process exited", ExitStatus}, S};
handle_info({'EXIT', _Port, Reason}, S=?S_MATCH) ->
    case Reason of
        normal ->
            {stop, normal, S};
        _ ->
            {stop, {port_exit, Reason}, S}
    end.

code_change(_, S, _) ->
    {ok, S}.

terminate(_, S) ->
    Port = ?S_PORT(S),
    case get_pid(Port) of
        undefined ->
            ok;
        Pid ->
            os:cmd("kill -TERM " ++ integer_to_list(Pid)),
            catch port_close(Port),
            ok
    end.

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Make sure that the data directory (passed in as `Dir') exists,
%% and that the `solr.xml' config file is in it.
-spec ensure_data_dir(string()) -> ok.
ensure_data_dir(Dir) ->
    SolrConfig = filename:join(Dir, ?YZ_SOLR_CONFIG_NAME),
    case filelib:is_file(SolrConfig) of
        true ->
            %% For future YZ releases, this path will probably need to
            %% check the existing solr.xml to see if it needs updates
            lager:debug("Existing solr config found, leaving it in place"),
            ok;
        false ->
            lager:info("No solr config found, creating a new one"),
            ok = filelib:ensure_dir(SolrConfig),
            {ok, _} = file:copy(?YZ_SOLR_CONFIG_TEMPLATE, SolrConfig),
            ok
    end.

-spec build_cmd(non_neg_integer(), non_neg_integer(), string()) -> {string(), [string()]}.
build_cmd(_SolrPort, _SolrJXMPort, "data/::yz_solr_start_timeout::") ->
    %% this will start an executable to keep yz_solr_proc's port
    %% happy, but will never respond to pings, so we can test the
    %% timeout capability
    {os:find_executable("grep"), ["foo"]};
build_cmd(SolrPort, SolrJMXPort, Dir) ->
    YZPrivSolr = filename:join([?YZ_PRIV, "solr"]),
    {ok, Etc} = application:get_env(riak_core, platform_etc_dir),
    Headless = "-Djava.awt.headless=true",
    SolrHome = "-Dsolr.solr.home=" ++ filename:absname(Dir),
    JettyHome = "-Djetty.home=" ++ YZPrivSolr,
    Port = "-Djetty.port=" ++ integer_to_list(SolrPort),
    CP = "-cp",
    CP2 = filename:join([YZPrivSolr, "start.jar"]),
    %% log4j.properties must be in the classpath unless a full URL
    %% (e.g. file://) is given for it, and we'd rather not put etc or
    %% data on the classpath, but we have to templatize the file to
    %% get the platform log directory into it
    Logging = "-Dlog4j.configuration=file://" ++
        filename:join([filename:absname(Etc), "solr-log4j.properties"]),
    LibDir = "-Dyz.lib.dir=" ++ filename:join([?YZ_PRIV, "java_lib"]),
    Class = "org.eclipse.jetty.start.Main",
    case SolrJMXPort of
        undefined ->
            JMX = [];
        _ ->
            JMXPortArg = "-Dcom.sun.management.jmxremote.port=" ++ integer_to_list(SolrJMXPort),
            JMXAuthArg = "-Dcom.sun.management.jmxremote.authenticate=false",
            JMXSSLArg = "-Dcom.sun.management.jmxremote.ssl=false",
            JMX = [JMXPortArg, JMXAuthArg, JMXSSLArg]
    end,

    Args = [Headless, JettyHome, Port, SolrHome, CP, CP2, Logging, LibDir]
        ++ string:tokens(solr_jvm_args(), " ") ++ JMX ++ [Class],
    {os:find_executable("java"), Args}.

%% @private
%%
%% @doc Get the operating system's PID of the Solr/JVM process.  May
%%      return `undefined' if Solr failed to start.
-spec get_pid(port()) -> undefined | pos_integer().
get_pid(Port) ->
    case erlang:port_info(Port) of
        undefined -> undefined;
        PI -> proplists:get_value(os_pid, PI)
    end.

%% @private
%%
%% @doc send a message to this server to remind it to check if solr
%% finished starting
schedule_solr_check(WaitTimeSecs) ->
    erlang:send_after(1000, self(), {check_solr, WaitTimeSecs-1}).

%% @private
%%
%% @doc Determine if Solr is running.
-spec is_up() -> boolean().
is_up() ->
    case yz_solr:cores() of
        {ok, _} -> true;
        _ -> false
    end.

run_cmd(Cmd, Args) ->
    open_port({spawn_executable, Cmd}, [exit_status, {args, Args}, use_stdio, stderr_to_stdout]).

solr_startup_wait() ->
    app_helper:get_env(?YZ_APP_NAME,
                       solr_startup_wait,
                       ?YZ_DEFAULT_SOLR_STARTUP_WAIT).

solr_jvm_args() ->
    app_helper:get_env(?YZ_APP_NAME,
                       solr_jvm_args,
                       ?YZ_DEFAULT_SOLR_JVM_ARGS).
