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

%% @doc This module/process is responsible for administrating the
%%      external Solr/JVM OS process.

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
         getpid/0,
         get_dist_query/0,
         set_dist_query/1]).

-record(state, {
          dir=exit(dir_undefined),
          port=exit(port_undefined),
          solr_port=exit(solr_port_undefined),
          solr_jmx_port=exit(solr_jmx_port_undefined),
          is_up=false,
          enable_dist_query=exit(enable_dist_query_undefined)
         }).

-define(SHUTDOWN_MSG, "INT\n").
-define(S_MATCH, #state{dir=_Dir,
                        port=_Port,
                        solr_port=_SolrPort,
                        solr_jmx_port=_SolrJMXPort}).
-define(S_PORT(S), S#state.port).
-define(YZ_DEFAULT_SOLR_JVM_OPTS, "").

-define(SOLRCONFIG_2_0_HASH, 64816669).
-define(LUCENE_MATCH_4_7_VERSION, "4.7").
-define(LUCENE_MATCH_4_10_4_VERSION, "4.10.4").

-type path() :: string().

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(string(), string(), string(), string()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Dir, TempDir, SolrPort, SolrJMXPort) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Dir, TempDir, SolrPort, SolrJMXPort], []).

%% @doc Get the operating system's PID of the Solr/JVM process.  May
%%      return `undefined' if Solr failed to start.
-spec getpid() -> undefined | pos_integer().
getpid() ->
    gen_server:call(?MODULE, getpid).

-spec get_dist_query() -> boolean().
get_dist_query() ->
    gen_server:call(?MODULE, enabled).

-spec set_dist_query(boolean()) -> {ok, PreviousValue :: boolean()} | {error, Reason :: term()}.
set_dist_query(Enabled) when is_boolean(Enabled) ->
    {ok, gen_server:call(?MODULE, {enabled, Enabled})};
set_dist_query(Value) ->
    {error, {bad_type, Value}}.

%%%===================================================================
%%% Callbacks
%%%===================================================================

%% NOTE: JVM startup is broken into two pieces:
%%       1. ensuring the data and temp dirs exist, and spawning the
%%          JVM process
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
init([Dir, TempDir, SolrPort, SolrJMXPort]) ->
    ensure_data_dir(Dir),
    ensure_temp_dir(TempDir),
    check_solr_index_versions(Dir),
    case get_java_path() of
        undefined ->
            %% This logging call is needed because the stop reason
            %% (which logs as a crash report) isn't always logged. My
            %% guess is that there is some race between shutdown and
            %% the logging.
            ?ERROR("unable to locate `java` on the PATH, shutting down"),
            {stop, "unable to locate `java` on the PATH"};
        JavaPath ->
            process_flag(trap_exit, true),
            {Cmd, Args} = build_cmd(JavaPath, SolrPort, SolrJMXPort, Dir, TempDir),
            ?INFO("Starting solr: ~p ~p", [Cmd, Args]),
            Port = run_cmd(Cmd, Args),
            schedule_solr_check(solr_startup_wait()),
            S = #state{
                   dir=Dir,
                   port=Port,
                   solr_port=SolrPort,
                   solr_jmx_port=SolrJMXPort,
                   enable_dist_query=?YZ_ENABLE_DIST_QUERY
                  },
            {ok, S}
    end.

handle_call(getpid, _, S) ->
    {reply, get_pid(?S_PORT(S)), S};
handle_call(enabled, _, #state{enable_dist_query=EnableDistQuery} = S) ->
    {reply, EnableDistQuery, S};
handle_call({enabled, NewValue}, _, #state{enable_dist_query=OldValue} = S) ->
    case {yz_solr:is_up(), OldValue, NewValue} of
        {true, false, true} ->
            riak_core_node_watcher:service_up(yokozuna, self());
        {_, true, false} ->
            riak_core_node_watcher:service_down(yokozuna);
        _ ->
            ok
    end,
    {reply, OldValue, S#state{enable_dist_query=NewValue}};
handle_call(Req, _, S) ->
    ?WARN("unexpected request ~p", [Req]),
    {noreply, S}.

handle_cast(Req, S) ->
    ?WARN("unexpected request ~p", [Req]),
    {noreply, S}.

handle_info({check_solr, WaitTimeSecs}, S=?S_MATCH) ->
    case {yz_solr:is_up(), ?YZ_ENABLE_DIST_QUERY} of
        {true, true} ->
            riak_core_node_watcher:service_up(yokozuna, self()),
            solr_is_up(S);
        {true, false} ->
            solr_is_up(S);
        {false, _} when WaitTimeSecs > 0 ->
            %% solr hasn't finished yet, keep waiting
            schedule_solr_check(WaitTimeSecs),
            {noreply, S};
        {false, _} ->
            %% solr did not finish startup quickly enough, or has just
            %% crashed and the exit message is on its way, shutdown
            {stop, "solr didn't start in alloted time", S}
    end;
handle_info(tick, #state{is_up=IsUp, enable_dist_query=EnableDistQuery} = S) ->
    NewIsUp = yz_solr:is_up(),
    case {IsUp, NewIsUp, EnableDistQuery} of
        {false, true, true} ->
            ?INFO("Solr was down but is now up.  Notifying cluster.", []),
            riak_core_node_watcher:service_up(yokozuna, self());
        {true, false, _} ->
            ?INFO("Solr was up but is now down.  Notifying cluster.", []),
            riak_core_node_watcher:service_down(yokozuna);
        _ -> ok
    end,
    schedule_tick(),
    {noreply, S#state{is_up=NewIsUp}};
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
    %% service_down is a gen_server:call with infinite timeout
    spawn(fun() -> riak_core_node_watcher:service_down(yokozuna) end),
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

%% @private
-spec build_cmd(filename(), non_neg_integer(), non_neg_integer(), string(), string()) ->
                       {string(), [string()]}.
build_cmd(_JavaPath, _SolrPort, _SolrJXMPort, "data/::yz_solr_start_timeout::", _TempDir) ->
    %% this will start an executable to keep yz_solr_proc's port
    %% happy, but will never respond to pings, so we can test the
    %% timeout capability
    {os:find_executable("grep"), ["foo"]};
build_cmd(JavaPath, SolrPort, SolrJMXPort, Dir, TempDir) ->
    YZPrivSolr = filename:join([?YZ_PRIV, "solr"]),
    {ok, Etc} = application:get_env(riak_core, platform_etc_dir),
    Headless = "-Djava.awt.headless=true",
    SolrHome = "-Dsolr.solr.home=" ++ filename:absname(Dir),
    HostContext = "-DhostContext=" ++ ?SOLR_HOST_CONTEXT,
    JettyHome = "-Djetty.home=" ++ YZPrivSolr,
    JettyTemp = "-Djetty.temp=" ++ filename:absname(TempDir),
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

    Args = [Headless, JettyHome, JettyTemp, Port, SolrHome, HostContext, CP, CP2, Logging, LibDir]
        ++ string:tokens(solr_jvm_opts(), " ") ++ JMX ++ [Class],
    {JavaPath, Args}.

%% @private
%%
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

%% @private
%%
%% @doc Recursively delete target directory. Equivalent to
%% `rm -rf $DIR` at the shell
-spec del_dir_recursive(string()) -> ok | {error, enotdir}.
del_dir_recursive(Path) ->
    del_dir_recursive(Path, filelib:is_dir(Path)).

-spec del_dir_recursive(string(), boolean()) -> ok | {error, enotdir}.
del_dir_recursive(DirPath, true) ->
    FullPaths = [filename:join(DirPath, FilePath) ||
                    FilePath <- filelib:wildcard("**", DirPath)],
    {Dirs, Files} = lists:partition(fun filelib:is_dir/1, FullPaths),
    lists:foreach(fun file:delete/1, Files),
    %% Delete directories sorted longest to shortest to ensure we delete leaf
    %% directories first.
    lists:foreach(fun file:del_dir/1, lists:sort(fun (A,B) -> A > B end, Dirs)),
    file:del_dir(DirPath);
del_dir_recursive(_DirPath, _Exists = false) ->
    {error, enotdir}.

%% @private
%%
%% @doc Make sure that the temp directory (passed in as `TempDir')
%% exists and is new
-spec ensure_temp_dir(string()) -> ok.
ensure_temp_dir(TempDir) ->
    del_dir_recursive(filename:join(TempDir, "solr-webapp")),
    ok = filelib:ensure_dir(filename:join(TempDir, empty)).

%% @private
%%
%% @doc Get the path of `java' executable.
-spec get_java_path() -> undefined | filename().
get_java_path() ->
    case os:find_executable("java") of
        false -> undefined;
        Val -> Val
    end.

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
-spec run_cmd(filename(), [string()]) -> port().
run_cmd(Cmd, Args) ->
    open_port({spawn_executable, Cmd}, [exit_status, {args, Args}, use_stdio, stderr_to_stdout]).

%% @private
%%
%% @doc send a message to this server to remind it to check if solr
%% finished starting
-spec schedule_solr_check(seconds()) -> reference().
schedule_solr_check(WaitTimeSecs) ->
    erlang:send_after(1000, self(), {check_solr, WaitTimeSecs-1}).

%% @private
%%
%% @doc Schedule next tick to be sent to this server.
-spec schedule_tick() -> ok.
schedule_tick() ->
    erlang:send_after(?YZ_SOLR_TICK_INTERVAL, self(), tick),
    ok.

%% @private
-spec solr_startup_wait() -> seconds().
solr_startup_wait() ->
    app_helper:get_env(?YZ_APP_NAME,
                       solr_startup_wait,
                       ?YZ_DEFAULT_SOLR_STARTUP_WAIT).

%% @private
-spec solr_jvm_opts() -> string().
solr_jvm_opts() ->
    app_helper:get_env(?YZ_APP_NAME,
                       solr_jvm_opts,
                       ?YZ_DEFAULT_SOLR_JVM_OPTS).

-spec solr_is_up(#state{}) -> {noreply, #state{}}.
solr_is_up(S) ->
    %% solr finished its startup, be merry
    ?INFO("solr is up", []),
    schedule_tick(),
    {noreply, S#state{is_up=true}}.

-spec check_solr_index_versions(path()) -> ok.
check_solr_index_versions(YZRootDir) ->
    DefaultSolrConfigPath = filename:join([?YZ_PRIV, "conf", "solrconfig.xml"]),
    DefaultSolrConfigHash = hash_file_contents(DefaultSolrConfigPath),
    SolrConfigIndexPaths = get_index_solrconfig_paths(YZRootDir),
    [check_index_solrconfig(SolrConfigIndexPath, DefaultSolrConfigPath, DefaultSolrConfigHash) ||
        SolrConfigIndexPath <- SolrConfigIndexPaths],
    ok.

-spec check_index_solrconfig(path(), path(), non_neg_integer()) -> ok.
check_index_solrconfig(SolrConfigIndexPath, DefaultSolrConfigPath, DefaultSolrConfigHash) ->
    case hash_file_contents(SolrConfigIndexPath) of
        DefaultSolrConfigHash ->
            ok;
        ?SOLRCONFIG_2_0_HASH ->
            upgrade_solr_config_file(
                DefaultSolrConfigPath,
                SolrConfigIndexPath,
                app_helper:get_env(
                    ?YZ_APP_NAME, ?YZ_UPGRADE_LUCENE_MATCH_VERSION,
                    ?YZ_UPGRADE_LUCENE_MATCH_VERSION_DEFAULT
                )
            ),
            lager:info(
                "Upgraded ~s to the latest version.", [SolrConfigIndexPath]
            );
        _ ->
            check_index_solrconfig_version(SolrConfigIndexPath)
    end.

-spec upgrade_solr_config_file(path(), path(), UpgradeLuceneMatchVersion::boolean()) -> ok.
upgrade_solr_config_file(DefaultSolrConfigPath, SolrConfigIndexPath, true) ->
    yz_misc:copy_files([DefaultSolrConfigPath], filename:dirname(SolrConfigIndexPath));
upgrade_solr_config_file(DefaultSolrConfigPath, SolrConfigIndexPath, false) ->
    DefaultSolrConfig = read_file(DefaultSolrConfigPath),
    UpdatedDefaultSolrConfig = replace_version(DefaultSolrConfig, "4.10.4", "4.7"),
    file:write_file(SolrConfigIndexPath, UpdatedDefaultSolrConfig).

-spec read_file(path()) -> string().
read_file(Path) ->
    {ok, Binary} = file:read_file(Path),
    erlang:binary_to_list(Binary).

-spec replace_version(string(), string(), string()) -> iolist().
replace_version(String, Substitutand, Substitution) ->
    Index = string:str(String, Substitutand),
    [
        string:substr(String, 1, Index - 1),
        Substitution,
        string:substr(String, Index + length(Substitutand))
    ].

-spec check_index_solrconfig_version(path()) -> ok.
check_index_solrconfig_version(SolrConfigIndexPath) ->
    case get_lucene_match_version(SolrConfigIndexPath) of
        ?LUCENE_MATCH_4_7_VERSION ->
            lager:warning(
                "The Solr configuration file ~s has been modified by the user and contains"
                " an outdated version (~s) under the luceneMatchVersion XML tag."
                "  Please consider reverting your changes or upgrading the luceneMatchVersion"
                " XML tag to ~s and restarting.  Note: In order to take full advantage of"
                " the changes, you should also reindex any data in this Solr core.",
                [SolrConfigIndexPath, ?LUCENE_MATCH_4_7_VERSION, ?LUCENE_MATCH_4_10_4_VERSION]);
        ?LUCENE_MATCH_4_10_4_VERSION ->
            ok;
        {error, no_lucene_match_version} ->
            lager:error(
                "The Solr configuration file ~s does not contain a luceneMatchVersion"
                " XML tag!",
                [SolrConfigIndexPath]);
        UnexpectedVersion ->
            lager:error(
                "The Solr configuration file ~s contains a luceneMatchVersion"
                " XML tag with an unexpected value: ~s",
                [SolrConfigIndexPath, UnexpectedVersion])
    end.

-spec get_index_solrconfig_paths(path()) -> [path()].
get_index_solrconfig_paths(YZRootDir) ->
    {ok, Files} = file:list_dir(YZRootDir),
    [YZRootDir ++ "/" ++ File ++ "/conf/solrconfig.xml" ||
        File <- Files,
        filelib:is_dir(YZRootDir ++ "/" ++ File)
            andalso has_solr_config(YZRootDir ++ "/" ++ File)].

-spec has_solr_config(path()) -> boolean().
has_solr_config(RootPath) ->
    SolrConfigPath = RootPath ++ "/conf/solrconfig.xml",
    filelib:is_file(SolrConfigPath) andalso not filelib:is_dir(SolrConfigPath).

-spec get_lucene_match_version(path()) -> string() | {error, no_lucene_match_version}.
get_lucene_match_version(SolrConfigIndexPath) ->
    {#xmlElement{content=Contents}, _Rest} = xmerl_scan:file(SolrConfigIndexPath),
    lucene_match_version(Contents).

-spec lucene_match_version([xmlElement()]) -> string() | {error, no_lucene_match_version}.
lucene_match_version([]) ->
    {error, no_lucene_match_version};
lucene_match_version(
    [#xmlElement{
        name=luceneMatchVersion,
        content=[#xmlText{value=Version}]
     } | _Rest]) ->
    Version;
lucene_match_version([_Element | Rest]) ->
    lucene_match_version(Rest).

-spec hash_file_contents(path()) -> non_neg_integer().
hash_file_contents(Path) ->
    {ok, Data} = file:read_file(Path),
    erlang:phash2(Data).
