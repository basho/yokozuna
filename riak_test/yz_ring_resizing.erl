-module(yz_ring_resizing).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("yokozuna.hrl").

%% @doc Test ring resizing while indexing and querying
%%
-define(FRUIT_SCHEMA_NAME, <<"fruit">>).
-define(BUCKET_TYPE, <<"data">>).
-define(INDEX, <<"fruit_index">>).
-define(INDEX_N_VAL, 4).
-define(BUCKET, {?BUCKET_TYPE, <<"fruit">>}).
-define(NUM_KEYS, 10000).
-define(SUCCESS, 0).
-define(CFG,
        [{riak_core,
          [
           %% Allow handoff to happen more quickly.
           {handoff_concurrency, 3},

           %% Use smaller ring size so that test runs faster.
           {ring_creation_size, 16},

           %% Reduce the tick so that ownership handoff will happen
           %% more quickly.
           {vnode_management_timer, 1000}
          ]},
         {yokozuna,
          [
           {enabled, true},

           %% Perform a full check every second so that non-owned
           %% postings are deleted promptly. This makes sure that
           %% postings are removed concurrent to async query during
           %% resize.
           {events_full_check_after, 2}
          ]}
        ]).
-define(SHRINK_SIZE, 32).
-define(EXPAND_SIZE, 64).

confirm() ->
    case yz_rt:bb_driver_setup() of
        {ok, YZBenchDir} ->
            random:seed(now()),

            %% build the 4 node cluster
            [ANode|_] = Cluster = rt:build_cluster(4, ?CFG),
            rt:wait_for_cluster_service(Cluster, yokozuna),
            PBConns = yz_rt:open_pb_conns(Cluster),

            %% Index and load data
            setup_indexing(Cluster, PBConns, YZBenchDir),
            {0, _} = yz_rt:load_data(Cluster, ?BUCKET, YZBenchDir, ?NUM_KEYS),
            %% wait for soft-commit
            timer:sleep(1000),

            %% Start a query and wait for it to start
            Ref1 = async_query(Cluster, YZBenchDir),
            timer:sleep(30000),

            %% Resize the ring -- size up, and make sure it completes
            lager:info("Resizing ring to ~p", [?EXPAND_SIZE]),
            submit_resize(?EXPAND_SIZE, ANode),
            ensure_ring_resized(Cluster),
            check_status(wait_for(Ref1)),
            pass;
        {error, bb_driver_build_failed} ->
            lager:info("Failed to build the yokozuna basho_bench driver"
                       " required for this test"),
            fail
    end.

%% The following section is commented out because ring-resizing downward currently
%% presents an unresolved issue in YZ. There is still value in the test, however,
%% and when the issue is resolve, this code should be un-commented.

    %% start another query
%%    Ref2 = async_query(Cluster, YZBenchDir),
%%    timer:sleep(10000),

    %% ring resize -- size down, and check it and query complete
%%    lager:info("resizing ring to ~p", [?SHRINK_SIZE]),
%%    submit_resize(?SHRINK_SIZE, ANode),
%%    ensure_ring_resized(Cluster),

%%    check_status(wait_for(Ref2)),
%%    yz_rt:close_pb_conns(PBConns),

async_query(Cluster, YZBenchDir) ->
    lager:info("Run async query against cluster ~p", [Cluster]),
    Hosts = yz_rt:host_entries(rt:connection_info(Cluster)),
    Concurrent = length(Hosts),
    Operations = [{{random_fruit_search, <<"_yz_id">>, 3, ?NUM_KEYS}, 1}],
    Cfg = [{mode, {rate,8}},
           {duration, 2},
           {concurrent, Concurrent},
           {code_paths, [YZBenchDir]},
           {driver, yz_driver},
           {operations, Operations},
           {http_conns, Hosts},
           {pb_conns, []},
           {bucket, ?BUCKET},
           {index, ?INDEX},
           {shutdown_on_error, true}],
    File = "bb-query-fruit",
    yz_rt:write_terms(File, Cfg),
    yz_rt:run_bb(async, File).

check_status({Status,_}) ->
    ?assertEqual(?SUCCESS, Status).

read_schema(YZBenchDir) ->
    Path = filename:join([YZBenchDir, "schemas", "fruit_schema.xml"]),
    {ok, RawSchema} = file:read_file(Path),
    RawSchema.

setup_indexing(Cluster, PBConns, YZBenchDir) ->
    Node = yz_rt:select_random(Cluster),
    PBConn = yz_rt:select_random(PBConns),

    yz_rt:create_bucket_type(Node, ?BUCKET_TYPE),

    RawSchema = read_schema(YZBenchDir),
    yz_rt:store_schema(PBConn, ?FRUIT_SCHEMA_NAME, RawSchema),
    yz_rt:wait_for_schema(Cluster, ?FRUIT_SCHEMA_NAME, RawSchema),
    ok = yz_rt:create_index(Node, ?INDEX, ?FRUIT_SCHEMA_NAME, ?INDEX_N_VAL),
    yz_rt:wait_for_index(Cluster, ?INDEX),
    yz_rt:set_index(Node, ?BUCKET, ?INDEX, ?INDEX_N_VAL).

wait_for(Ref) ->
    rt:wait_for_cmd(Ref).

submit_resize(NewSize, Node) ->
    ?assertEqual(ok, rpc:call(Node, riak_core_claimant, resize_ring, [NewSize])),
    {ok, _, _} = rpc:call(Node, riak_core_claimant, plan, []),
    ?assertEqual(ok, rpc:call(Node, riak_core_claimant, commit, [])).

ensure_ring_resized(Cluster) ->
    IsResizeComplete =
        fun(Node) ->
            lager:debug("Waiting for is_resize_complete on node ~p", [Node]),
            Ring = rpc:call(Node, yz_misc, get_ring, [transformed]),
            rpc:call(Node, riak_core_ring, is_resize_complete, [Ring])
        end,
    ?assertEqual(ok, yz_rt:wait_until(Cluster, IsResizeComplete)).
