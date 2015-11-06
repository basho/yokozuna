%% @doc Test the startup and shutdown sequence.
-module(yz_startup_shutdown).
-export([confirm/0]).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CONFIG, [{yokozuna, [{enabled, true}]}]).

confirm() ->
    Cluster = rt:build_cluster(4, ?CONFIG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    pass.

