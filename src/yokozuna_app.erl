-module(yokozuna_app).
-behaviour(application).
-include("yokozuna.hrl").
-export([start/2, stop/1]).


%%%===================================================================
%%% Callbacks
%%%===================================================================

start(_StartType, _StartArgs) ->
    riak_core:wait_for_service(riak_kv),
    ok = start_solr(?YZ_ROOT_DIR),
    case yokozuna_sup:start_link() of
        {ok, Pid} ->
            register_app(),
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    ok.


%%%===================================================================
%%% Private
%%%===================================================================

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
