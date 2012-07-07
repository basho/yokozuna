-module(yz_solr_proc_sup).
-behavior(supervisor).
-include("yokozuna.hrl").
-compile(export_all).
-export([init/1,
         start_link/0]).


%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_Args) ->
    Dir = ?YZ_ROOT_DIR,
    SolrPort = yz_solr:port(),
    SolrProc = {yz_solr_proc,
                {yz_solr_proc, start_link, [Dir, SolrPort]},
                permanent, 5000, worker, [yz_solr_proc]},

    {ok, {{one_for_one, 5, 60}, [SolrProc]}}.
