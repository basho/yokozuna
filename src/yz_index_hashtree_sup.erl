-module(yz_index_hashtree_sup).
-behavior(supervisor).
-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    %% TODO: should shutdown be longer to account for leveldb?
    Spec = {ignored,
            {yz_index_hashtree, start_link, []},
            temporary, 5000, worker, [yz_index_hashtree]},
    {ok, {{simple_one_for_one, 10, 1}, [Spec]}}.
