%% @doc Test the startup and shutdown sequence.
-module(yz_startup_shutdown).
-export([confirm/0]).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile({parse_transform, rt_intercept_pt}).

-define(CONFIG, [{yokozuna, [{enabled, true}]}]).
-define(YZ_SERVICES, [yz_pb_search, yz_pb_admin]).

confirm() ->
    Cluster = rt:build_cluster(2, ?CONFIG),
    rt:wait_for_cluster_service(Cluster, yokozuna),

    verify_yz_components_enabled(Cluster),
    verify_yz_services_registered(Cluster),

    intercept_yz_solrq_drain_mgr_drain(Cluster),
    stop_yokozuna(Cluster),

    verify_drain_called(Cluster),
    verify_yz_components_disabled(Cluster),
    verify_yz_services_deregistered(Cluster),
    pass.

%% @private
%%
%% @doc Assert that all components are enabled on each node in `Cluster'.
verify_yz_components_enabled(Cluster) ->
    check_yz_components(Cluster, true).

%% @private
%%
%% @doc Assert that all components are disabled on each node in `Cluster'.
verify_yz_components_disabled(Cluster) ->
    check_yz_components(Cluster, false).

%% @private
%%
%% @doc Checks that the enabled status of all yokozuna components is equal to
%% `Enabled'.
check_yz_components([], _Enabled) ->
    ok;
check_yz_components([Node|Rest], Enabled) ->
    Components = yz_app:components(),
    lists:all(
      fun(Component) ->
              Enabled =:= rpc:call(Node, yokozuna, is_enabled, [Component])
      end,
      Components),
    check_yz_components(Rest, Enabled).

%% @private
%%
%% @doc Assert that all services are registerd on each node in `Cluster'.
verify_yz_services_registered(Cluster) ->
    lists:all(
      fun(Node) ->
              true =:= are_services_registered(?YZ_SERVICES, Node)
      end,
      Cluster).

%% @private
%%
%% @doc Assert that all services are registerd on each node in `Cluster'.
verify_yz_services_deregistered(Cluster) ->
    lists:all(
      fun(Node) ->
              false =:= are_services_registered(?YZ_SERVICES, Node)
      end,
      Cluster).

%% @private
%%
%% @doc Are the given `Services' currently registered on `Node'?
-spec are_services_registered(Services::[atom()], Node::node()) -> boolean().
are_services_registered(Services, Node) ->
    RegisteredServices = lists:flatten(
                           rpc:call(Node, riak_api_pb_registrar, services, [])),
    lists:all(
      fun(Service) ->
              lists:member(Service, RegisteredServices)
      end,
      Services).

%% @private
%%
%% @doc Install an intercept on all of the given nodes, which intercepts the
%% yz_solrq_drain_mgr:drain/0 function and sends a message to the riak_test
%% process indicating that the function was actually called on a given node.
%% The message is of the form {Node, drain_called}, where `Node' identifies the
%% node.
intercept_yz_solrq_drain_mgr_drain([]) ->
    ok;
intercept_yz_solrq_drain_mgr_drain([Node|Rest]) ->
    RiakTestProcess = self(),
    rt_intercept:add(
      Node,
      {yz_solrq_drain_mgr,
       [{{drain, 0},
         {[Node, RiakTestProcess],
          fun() ->
                  RiakTestProcess ! {Node, drain_called}
          end}}]}),
    intercept_yz_solrq_drain_mgr_drain(Rest).

%% @private
%%
%% @doc Stop the yokozuna application on all of the given nodes.
stop_yokozuna([]) ->
    ok;
stop_yokozuna([Node|Rest]) ->
    ok = rpc:call(Node, application, stop, [yokozuna]),
    stop_yokozuna(Rest).

%% @private
%%
%% @doc For each node in `Cluster', wait for a message of the form
%% {Node, drain_called} and assert that each was received before
%% timeout.
verify_drain_called(Cluster) ->
    Results = [begin
                   receive
                       {Node, drain_called} ->
                           ok
                   after
                       10000 ->
                           {fail, timeout}
                   end
               end
               || Node <- Cluster],

    true = lists:all(fun(Result) ->
                             ok =:= Result
                     end,
                     Results).
