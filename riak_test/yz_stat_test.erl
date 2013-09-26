%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
%%
%% @doc Check that stats run as expected
-module(yz_stat_test).
-compile(export_all).
-import(yz_rt, [host_entries/1,
                wait_for_joins/1,
                join_all/1,
                select_random/1]).
-include_lib("eunit/include/eunit.hrl").

-define(CFG, [{yokozuna, [{enabled, true}]}]).

confirm() ->
    random:seed(now()),
    Cluster = prepare_cluster(1),
    confirm_stats(Cluster),
    pass.

prepare_cluster(NumNodes) ->
    Nodes = rt:deploy_nodes(NumNodes, ?CFG),
    Cluster = join_all(Nodes),
    wait_for_joins(Cluster),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    Cluster.

create_indexed_bucket(Pid, Cluster, IndexBucket) ->
    create_indexed_bucket(Pid, Cluster, IndexBucket, IndexBucket).

create_indexed_bucket(Pid, Cluster, Index, Bucket) ->
    ?assertEqual(ok, riakc_pb_socket:create_search_index(Pid, Index)),
    ?assertEqual(ok, riakc_pb_socket:set_bucket(Pid, Bucket, [{yz_index, Index}])),
    yz_rt:wait_for_index(Cluster, binary_to_list(Index)),
    ok.

%% populate random plain text values
populate_data(_, _, 0, Acc) -> Acc;
populate_data(Pid, IB, Count, Acc)-> 
    KV = gen_random_name(16),
    PO = riakc_obj:new(IB, KV, KV, "text/plain"),
    {ok, _Obj} = riakc_pb_socket:put(Pid, PO, [return_head]),
    populate_data(Pid, IB, Count - 1, [KV|Acc]).

populate_data_and_wait(Pid, Cluster, IndexBucket, Count) ->
    Values = populate_data(Pid, IndexBucket, Count, []),
    Search = <<"text:*">>,
    F = fun(_) ->
                {ok,{search_results,R,Score,Found}} =
                    riakc_pb_socket:search(Pid, IndexBucket, Search, []),
                (Count == Found) and (Score =/= 0.0)
        end,
    yz_rt:wait_until(Cluster, F),
    Values.

%% search for a list of values assumign plain text
search_values(_Pid, _IndexBucket, []) -> ok;
search_values(Pid, IndexBucket, [Value|Rest]) ->
    riakc_pb_socket:search(Pid, IndexBucket, <<"text:", Value/binary>>, []),
    search_values(Pid, IndexBucket, Rest).

gen_random_name(Length) ->
    Chars = "abcdefghijklmnopqrstuvwxyz1234567890",
    Value = lists:foldl(fun(_, Acc) ->
            [lists:nth(random:uniform(length(Chars)), Chars)] ++ Acc
        end, [], lists:seq(1, Length)),
    list_to_binary(Value).

confirm_stats(Cluster) ->
    {Host, Port} = select_random(host_entries(rt:connection_info(Cluster))),
    IndexBucket = gen_random_name(16),

    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    create_indexed_bucket(Pid, Cluster, IndexBucket),
    Values = populate_data_and_wait(Pid, Cluster, IndexBucket, 10),
    search_values(Pid, IndexBucket, Values),
    riakc_pb_socket:stop(Pid),

    Node = select_random(Cluster),
    Stats = rpc:call(Node, yz_stat, get_stats, []),

    ILatency = proplists:get_value(index_latency, Stats, []),
    ?assert(proplists:get_value(min, ILatency, 0) > 0),
    ?assert(proplists:get_value(max, ILatency, 0) > 0),
    ?assert(proplists:get_value(standard_deviation, ILatency, 0) > 0),

    IThroughput = proplists:get_value(index_throughput, Stats, []),
    ?assert(proplists:get_value(count, IThroughput, 0) > 0),
    ?assert(proplists:get_value(one, IThroughput, 0) > 0),

    SThroughput = proplists:get_value(search_throughput, Stats, []),
    ?assert(proplists:get_value(count, SThroughput, 0) > 0),
    ?assert(proplists:get_value(one, SThroughput, 0) > 0),
    ok.
