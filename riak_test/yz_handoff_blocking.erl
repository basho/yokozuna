%% @doc Verify yokozuna cannot block kv handoff forever
-module(yz_handoff_blocking).
-compile(nowarn_export_all).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(INDEX, <<"handoff_blocking">>).
-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 16}
          ]},
	 {yokozuna,
	  [
	   {enabled, true}
	  ]}
        ]).

confirm() ->
    [Node, Node2] = Cluster = rt:deploy_nodes(2, ?CFG),

    %% create an index on one node and populate it with some data
    yz_rt:create_index([Node], ?INDEX),
    ok = yz_rt:set_bucket_type_index([Node], ?INDEX),
    ConnInfo = yz_rt:connection_info([Node]),
    {Host, Port} = yz_rt:riak_http(proplists:get_value(Node, ConnInfo)),
    URL = ?FMT("http://~s:~s/types/~s/buckets/~s/keys/~s",
               [Host, integer_to_list(Port), ?INDEX, <<"bucket">>, <<"key">>]),
    Headers = [{"content-type", "text/plain"}],
    Body = <<"yokozuna">>,
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body, []),

    %% load and install the intercept
    rt_intercept:load_code(Node2, [filename:join([rt_config:get(yz_dir), "riak_test", "intercepts", "*.erl"])]),
    rt_intercept:add(Node2, {yz_solr, [{{cores,0}, slow_cores}]}),

    %% join a node
    rt:join_cluster(Cluster),
    ok = rt:wait_until_no_pending_changes(Cluster),
    yz_rt:wait_for_index(Cluster, ?INDEX),
    pass.
