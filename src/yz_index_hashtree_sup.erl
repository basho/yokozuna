-module(yz_index_hashtree_sup).
-behavior(supervisor).
-include("yokozuna.hrl").
-compile(export_all).
-export([init/1]).

%% @doc Get the list of trees.
-spec trees() -> ['restarting' | 'undefined' | pid()].
trees() ->
    Children = supervisor:which_children(?MODULE),
    [Pid || {_,Pid,_,_} <- Children].

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    Spec = {ignored,
            {yz_index_hashtree, start_link, []},
            temporary, 5000, worker, [yz_index_hashtree]},
    {ok, {{simple_one_for_one, 10, 1}, [Spec]}}.
