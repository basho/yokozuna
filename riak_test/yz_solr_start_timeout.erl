%% @doc Ensure that if Solr doesn't start before the startup wait,
%% yokozuna tears down the Riak node.
%%
%% Note: if this test is successful, the node will be left in a state
%% that will never run solr successfully (because the WAR file will be
%% missing).
-module(yz_solr_start_timeout).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(CFG, [{yokozuna, [{enabled, true}]}]).

confirm() ->
    %% this root_dir is a well-known name that causes yz_solr_proc to
    %% start something that will trigger yz_solr_proc's timeout
    Crippled = {root_dir, "data/::yz_solr_start_timeout::"},
    [Node|_] = rt:deploy_nodes(1, [{yokozuna, [{enabled, true},Crippled]}]),

    %% node should start up successfully, but solr never will, so...
    rt:start_and_wait(Node),

    %% ... it should die in a bit
    ok = rt:wait_until_unpingable(Node),

    %% if it doesn't, we'll never get to this point, because
    %% the wait asserts failure on timeout

    %% if we did get here, check the log to make sure it was the
    %% startup wait that triggered

    Logs = rt:get_node_logs(),
    ?assert(find_startup_wait_log(Logs)),
    pass.

%% Find "solr didn't start in alloted time" in console.log
find_startup_wait_log([]) ->
    false;
find_startup_wait_log([{Path, Port}|Rest]) ->
    case re:run(Path, "console\.log$") of
        {match, _} ->
            find_line(Port, file:read_line(Port));
        nomatch ->
            find_startup_wait_log(Rest)
    end.

find_line(Port, {ok, Data}) ->
    case re:run(Data, "solr didn't start in alloted time") of
        {match, _} ->
            true;
        nomatch ->
            find_line(Port, file:read_line(Port))
    end.
