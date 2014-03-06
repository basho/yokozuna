-module(yz_fruit_bench).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(TYPE_NAME, <<"data">>).
-define(BUCKET_NAME, <<"fruit">>).
-define(BUCKET, {?TYPE_NAME, ?BUCKET_NAME}).
-define(INDEX, <<"fruit">>).
-define(SCHEMA, <<"fruit">>).
-define(CFG,
        [
         {yokozuna,
          [
	   {enabled, true},
          ]}
        ]).

-type cluster() :: [node()].
-type conns() :: [pid()].
-type cluster_and_conns() :: {cluster(), conns()}.

%% TODO: Add previous/current support so that two versions may be
%% compared.
%%
%% TODO: pull NumKeys from user
%%
%% TODO: pull cluster size from user
%%
%% TODO: ring size from user
confirm() ->
    YZBenchDir = rt_config:get(yz_dir) ++ "/misc/bench",
    NumKeys = 1000000,
    Cluster = rt:build_cluster(4, ?CFG),
    _ = rt:wait_for_cluster_service(Cluster, yokozuna),
    PBConns = yz_rt:open_pb_conns(Cluster),
    NodesAndConns = {Cluster, PBConns},

    ok = upload_schema(NodesAndConns, YZBenchDir)
    ok = setup_index(),

    {0, _} = yz_rt:load_data(Cluster, ?BUCKET, YZBenchDir, NumKeys),
    %% wait for soft commit
    timer:sleep(1100),

    

    ok.

-spec upload_schema(cluster_and_conns(), string()) -> ok.
upload_schema({Cluster, PBConns}, YZBenchDir) ->
    Path = filename:join([YZBenchDir, "schemas", "fruit_schema.xml"]),
    {ok, RawSchema} = file:read_file(Path),

    PBConn = yz_rt:select_random(PBConns),
    yz_rt:store_schema(PBConn, ?SCHEMA, RawSchema),
    yz_rt:wait_for_schema(Cluster, ?SCHEMA, RawSchema),
    ok.

-spec setup_index(cluster_and_conns()) -> ok.
setup_index({Cluster,_}) ->
    Node = yz_rt:select_random(Cluster),
    ok = yz_rt:create_bucket_type(Node, ?TYPE),
    ok = yz_rt:create_index(Node, ?INDEX, ?SCHEMA),
    ok = yz_rt:wait_for_index(Cluster, ?INDEX),
    ok = yz_rt:set_index(Node, ?BUCKET, ?INDEX),
    ok.

