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


%%%===================================================================
%%% Callbacks
%%%===================================================================

%% NOTE: Doing the work here will slow down startup but I think that's
%%       desirable given that Solr must be up for Yokozuna to work
%%       properly.
init([Dir, SolrPort, SolrJMXPort]) ->
    process_flag(trap_exit, true),
    {Cmd, Args} = build_cmd(SolrPort, SolrJMXPort, Dir),
    ?INFO("Starting solr: ~p ~p", [Cmd, Args]),
    Port = run_cmd(Cmd, Args),
    wait_for_solr(solr_startup_wait()),
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

handle_info({_Port, {data, Data}}, S=?S_MATCH) ->
    ?DEBUG("~p", Data),
    {noreply, S};

handle_info({exit_status, ExitStatus}, S) ->
    exit({"solr OS process exited", ExitStatus, S}).

code_change(_, S, _) ->
    {ok, S}.

terminate(_, S) ->
    Port = ?S_PORT(S),
    os:cmd("kill -TERM " ++ integer_to_list(get_pid(Port))),
    port_close(Port),
    ok.

getpid() ->
    gen_server:call(?MODULE, getpid).

%%%===================================================================
%%% Private
%%%===================================================================

-spec build_cmd(string(), string(), string()) -> {string(), [string()]}.
build_cmd(SolrPort, SolrJMXPort, Dir) ->
    Headless = "-Djava.awt.headless=true",
    SolrHome = "-Dsolr.solr.home=" ++ Dir,
    JettyHome = "-Djetty.home=" ++ Dir,
    Port = "-Djetty.port=" ++ SolrPort,
    CP = "-cp",
    CP2 = "./" ++ Dir ++ "/start.jar:./" ++ Dir,
    Logging = "-Dlog4j.configuration=log4j.properties",
    LibDir = "-Dyz.lib.dir=" ++ filename:join([?YZ_PRIV, "java_lib"]),
    Class = "org.eclipse.jetty.start.Main",
    case SolrJMXPort of
        undefined ->
            JMX = [];
        _ ->
            JMXPortArg = "-Dcom.sun.management.jmxremote.port=" ++ SolrJMXPort,
            JMXAuthArg = "-Dcom.sun.management.jmxremote.authenticate=false",
            JMXSSLArg = "-Dcom.sun.management.jmxremote.ssl=false",
            JMX = [JMXPortArg, JMXAuthArg, JMXSSLArg]
    end,

    Args = [Headless, JettyHome, Port, SolrHome, CP, CP2, Logging, LibDir]
        ++ solr_vm_args() ++ JMX ++ [Class],
    {os:find_executable("java"), Args}.

-spec get_pid(port()) -> pos_integer().
get_pid(Port) ->
    proplists:get_value(os_pid, erlang:port_info(Port)).

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

solr_vm_args() ->
    app_helper:get_env(?YZ_APP_NAME,
                       solr_vm_args,
                       ?YZ_DEFAULT_SOLR_VM_ARGS).

wait_for_solr(0) ->
    throw({error, "Solr didn't start in alloted time"});
wait_for_solr(N) ->
    case is_up() of
        true ->
            ok;
        false ->
            timer:sleep(1000),
            wait_for_solr(N-1)
    end.
