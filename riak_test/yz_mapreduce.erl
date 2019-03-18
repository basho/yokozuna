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
    Bucket = {Index, <<"b1">>},
    Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    yz_rt:create_index(Cluster, Index),
    yz_rt:set_bucket_type_index(Cluster, Index),
    timer:sleep(500),
    yz_rt:write_objs(Cluster, Bucket),
    verify_objs_mr(Cluster, Index),
    ok = yz_rt:load_module(Cluster, ?MODULE),
    %% NOTE: Deliberate choice not to use `wait_unil'.  The data is
    %% known to be indexed at this point; therefore the fold should
    %% ALWAYS return the full unique set of results.
    _ = [ok = verify_unique(Node, Index) || Node <- Cluster],
    pass.

%% @doc Verify `yokozuna:search_fold' returns the total set of unique
%% results.  This check is related to issue #355.  Pagination can
%% break depending on the `sort' parameter.
%%
%% https://github.com/basho/yokozuna/issues/355
-spec verify_unique(node(), index_name()) -> ok.
verify_unique(Node, Index) ->
    lager:info("Verify search_fold on ~s returns entire unique result set",
               [Node]),
    Args = [Index, <<"name_s:yokozuna">>, <<"">>,
            fun collect_results/2, {Index, []}],
    {_, Results} = rpc:call(Node, yokozuna, search_fold, Args),
    ?assertEqual(1000, length(lists:usort(Results))),
    ok.

%% @private
%%
%% @doc Collect the results from `yokozuna:search_fold'.  This
%% function is running on a server process, not the riak test process.
collect_results(_, {_, failed_to_change_query_plan}) ->
    failed_to_change_query_plan;
collect_results(Results, {Index, Acc}) ->
    case change_query_plan(Index) of
        ok ->
            {Index, Acc ++ Results};
        fail ->
            {Index, failed_to_change_query_plan}
    end.

%% @private
%%
%% @doc Wait for the query plan to change.  This function runs on a
%% server process, not the riak test process.
-spec change_query_plan(index_name()) -> ok | fail.
change_query_plan(Index) ->
    change_query_plan(Index, 10).

change_query_plan(_, 0) ->
    fail;
change_query_plan(Index, Num) ->
    Plan1 = yz_cover:plan(Index),
    gen_server:cast(yz_cover, update_all_plans),
    %% NOTE: this sleep is here to give the server a chance to
    %% update the cache, dont' remove it.
    timer:sleep(500),
    Plan2 = yz_cover:plan(Index),
    case Plan1 /= Plan2 of
        true ->
            ok;
        false ->
            change_query_plan(Index, Num - 1)
    end.

-spec verify_objs_mr(list(), index_name()) -> ok.
verify_objs_mr(Cluster, Index) ->
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
                A = hd(mochijson2:decode(http_mr(HP, MR))),
                lager:info("Running map-reduce job on ~p", [Node]),
                lager:info("E: 1000, A: ~p", [A]),
                1000 == A
        end,
    yz_rt:wait_until(Cluster, F).

-spec http_mr({yz_rt:host(), yz_rt:portnum()}, term()) -> binary().
http_mr({Host,Port}, MR) ->
    URL = ?FMT("http://~s:~s/mapred", [Host, integer_to_list(Port)]),
    Opts = [],
    Headers = [{"content-type", "application/json"}],
    Body = mochijson2:encode(MR),
    {ok, "200", _, RBody} = ibrowse:send_req(URL, Headers, post, Body, Opts),
    RBody.
