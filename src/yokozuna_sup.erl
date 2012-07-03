-module(yokozuna_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).


%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_Args) ->
    VMaster = {yokozuna_vnode_master,
               {riak_core_vnode_master, start_link, [yokozuna_vnode]},
               permanent, 5000, worker, [riak_core_vnode_master]},
    Events = {yz_events,
              {yz_events, start_link, []},
              permanent, 5000, worker, [yz_events]},

    {ok, {{one_for_one, 5, 10}, [VMaster, Events]}}.
