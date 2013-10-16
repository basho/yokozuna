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

create_indexed_bucket(Pid, Cluster, Index) ->
    ?assertEqual(ok, riakc_pb_socket:create_search_index(Pid, Index)),
    yz_rt:set_bucket_type_index(hd(Cluster), Index),
    yz_rt:wait_for_index(Cluster, Index),
    ok.

%% populate random plain text values
populate_data(_, _, 0, Acc) -> Acc;
populate_data(Pid, Bucket, Count, Acc)->
    KV = gen_random_name(16),
    PO = riakc_obj:new(Bucket, KV, KV, "text/plain"),
    {ok, _Obj} = riakc_pb_socket:put(Pid, PO, [return_head]),
    populate_data(Pid, Bucket, Count - 1, [KV|Acc]).

populate_data_and_wait(Pid, Cluster, Bucket, Index, Count) ->
    Values = populate_data(Pid, Bucket, Count, []),
    Search = <<"text:*">>,
    F = fun(_) ->
                {ok,{search_results,_R,Score,Found}} =
                    riakc_pb_socket:search(Pid, Index, Search, []),
                (Count == Found) and (Score =/= 0.0)
        end,
    yz_rt:wait_until(Cluster, F),
    Values.

%% search for a list of values assumign plain text
search_values(_Pid, _Index, []) -> ok;
search_values(Pid, Index, [Value|Rest]) ->
    riakc_pb_socket:search(Pid, Index, <<"text:", Value/binary>>, []),
    search_values(Pid, Index, Rest).

gen_random_name(Length) ->
    Chars = "abcdefghijklmnopqrstuvwxyz1234567890",
    Value = lists:foldl(fun(_, Acc) ->
            [lists:nth(random:uniform(length(Chars)), Chars)] ++ Acc
        end, [], lists:seq(1, Length)),
    list_to_binary(Value).

write_bad_json(_, _, 0) ->
    ok;
write_bad_json(Pid, Bucket, Num) ->
    Key = list_to_binary("bad_json_" ++ integer_to_list(Num)),
    Value = <<"{\"bad\": \"unclosed\"">>,
    PO = riakc_obj:new(Bucket, Key, Value, "application/json"),
    {ok, _Obj} = riakc_pb_socket:put(Pid, PO, [return_head]).

confirm_stats(Cluster) ->
    {Host, Port} = select_random(host_entries(rt:connection_info(Cluster))),
    Index = gen_random_name(16),
    Bucket = {Index, <<"b1">>},

    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    create_indexed_bucket(Pid, Cluster, Index),
    Values = populate_data_and_wait(Pid, Cluster, Bucket, Index, 10),
    search_values(Pid, Index, Values),
    write_bad_json(Pid, Bucket, 10),
    riakc_pb_socket:stop(Pid),

    yz_rt:wait_until(Cluster, fun check_stat_values/1).

-define(STAT_NAME(Path), yz_stat:stat_name(Path)).

check_stat_values(Node) ->
    Stats = rpc:call(Node, yz_stat, get_stats, []),
    lager:info("STATS: ~p", [Stats]),

    ILatency = proplists:get_value(?STAT_NAME([index, latency]), Stats),
    ILatencyMin = proplists:get_value(min, ILatency),
    ILatencyMax = proplists:get_value(max, ILatency),

    IThroughput = proplists:get_value(?STAT_NAME([index, throughput]), Stats),
    IThruCount = proplists:get_value(count, IThroughput),
    IThruOne = proplists:get_value(one, IThroughput),

    SThroughput = proplists:get_value(?STAT_NAME([search, throughput]), Stats),
    SThruCount = proplists:get_value(count, SThroughput),
    SThruOne = proplists:get_value(one, SThroughput),

    IFail = proplists:get_value(?STAT_NAME([index, fail]), Stats),
    IFailCount = proplists:get_value(count, IFail),
    IFailOne = proplists:get_value(one, IFail),

    Pairs = [{index_latency_min, ILatencyMin, '>', 0},
             {index_latency_max, ILatencyMax, '>', 0},
             {index_throughput_count, IThruCount, '>', 0},
             {index_throughput_one, IThruOne, '>', 0},
             {search_throughput_count, SThruCount, '>', 0},
             {search_throughput_cone, SThruOne, '>', 0},
             {index_fail_count, IFailCount, '>', 0},
             {index_fail_one, IFailOne, '>', 0}],
    lager:info("Stats: ~p", [Pairs]),
    StillWaiting = [S || S={_, Value, Cmp, Arg} <- Pairs, not (erlang:Cmp(Value, Arg))],
    case StillWaiting of
        [] ->
            true;
        _ ->
            lager:info("Waiting for stats: ~p", [StillWaiting]),
            false
    end.
