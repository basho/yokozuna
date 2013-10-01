%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
%%
%% @doc Test Yokozuna's map/reduce integration.
-module(yz_mapreduce).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("yokozuna.hrl").
-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).

-type host() :: string().
-type portnum() :: integer().

-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 16}
          ]},
         {riak_kv,
          [
           %% make handoff happen faster
           {handoff_concurrency, 16},
           {inactivity_timeout, 2000}
          ]},
         {yokozuna,
          [
	   {enabled, true}
          ]}
        ]).

-spec confirm() -> pass.
confirm() ->
    Index = <<"mr_index">>,
    random:seed(now()),
    Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    yz_rt:create_index(yz_rt:select_random(Cluster), Index),
    yz_rt:set_index(yz_rt:select_random(Cluster), Index),
    yz_rt:wait_for_index(Cluster, Index),
    write_100_objs(Cluster, Index),
    verify_100_objs_mr(Cluster, Index),
    pass.

-spec verify_100_objs_mr(list(), string()) -> ok.
verify_100_objs_mr(Cluster, Index) ->
    MakeTick = [{map, [{language, <<"javascript">>},
                       {keep, false},
                       {source, <<"function(v) { return [1]; }">>}]}],
    ReduceSum = [{reduce, [{language, <<"javascript">>},
                           {keep, true},
                           {name, <<"Riak.reduceSum">>}]}],
    MR = [{inputs, [{module, <<"yokozuna">>},
                    {function, <<"mapred_search">>},
                    {arg, [Index, <<"name_s:yokozuna">>]}]},
          {'query', [MakeTick, ReduceSum]}],
    F = fun(Node) ->
                HP = hd(yz_rt:host_entries(rt:connection_info([Node]))),
                R = http_mr(HP, MR),
                100 == hd(mochijson2:decode(R))
        end,
    yz_rt:wait_until(Cluster, F).

-spec write_100_objs([node()], index_name()) -> ok.
write_100_objs(Cluster, Index) ->
    lager:info("Writing 100 objects"),
    lists:foreach(write_obj(Cluster, Index), lists:seq(1,100)).

-spec write_obj([node()], index_name()) -> fun().
write_obj(Cluster, Index) ->
    fun(N) ->
            PL = [{name_s,<<"yokozuna">>}, {num_i,N}],
            Key = list_to_binary(io_lib:format("key_~B", [N])),
            Body = mochijson2:encode(PL),
            HP = yz_rt:select_random(yz_rt:host_entries(rt:connection_info(Cluster))),
            CT = "application/json",
            lager:info("Writing object with bkey ~p [~p]", [{Index,Key}, HP]),
            yz_rt:http_put(HP, Index, Key, CT, Body)
    end.

-spec http_mr({host(), portnum()}, term()) -> binary().
http_mr({Host,Port}, MR) ->
    lager:info("Running map-reduce job [~p]", [{Host,Port}]),
    URL = ?FMT("http://~s:~s/mapred", [Host, integer_to_list(Port)]),
    Opts = [],
    Headers = [{"content-type", "application/json"}],
    Body = mochijson2:encode(MR),
    {ok, "200", _, RBody} = ibrowse:send_req(URL, Headers, post, Body, Opts),
    RBody.
