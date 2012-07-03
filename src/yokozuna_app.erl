-module(yokozuna_app).
-behaviour(application).
-export([start/2, stop/1]). % prevent compile warnings
-compile(export_all).
-include("yokozuna.hrl").


%%%===================================================================
%%% Callbacks
%%%===================================================================

start(_StartType, _StartArgs) ->
    riak_core:wait_for_service(riak_kv),
    ok = start_solr(?YZ_ROOT_DIR),
    case yokozuna_sup:start_link() of
        {ok, Pid} ->
            register_app(),
            add_routes(wm_routes()),
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    ok.


%%%===================================================================
%%% Private
%%%===================================================================

add_routes(Routes) ->
    [webmachine_router:add_route(R) || R <- Routes].

register_app() ->
    Modules = [{vnode_module, yokozuna_vnode}],
    riak_core:register(yokozuna, Modules).

start_solr(Dir) ->
    case yokozuna_solr:ping() of
        true -> ok;
        %% TODO: start_solr is currently linking to process running
        %% this
        false -> ok = yokozuna_solr:start(Dir)
    end.

wm_routes() ->
    [{["search", index], yz_wm_search, []}].
