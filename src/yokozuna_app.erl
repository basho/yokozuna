-module(yokozuna_app).
-behaviour(application).

-export([start/2, stop/1]).


%%%===================================================================
%%% Callbacks
%%%===================================================================

start(_StartType, _StartArgs) ->
    riak_core:wait_for_service(riak_kv),

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
