-module(yokozuna).
-include("yokozuna.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-compile(export_all).


%%%===================================================================
%%% API
%%%===================================================================

%% @doc Index the given object `O'.
-spec index(string(), riak_object:riak_object()) -> ok | {error, term()}.
index(Index, O) ->
    yz_solr:index(Index, [yz_doc:make_doc(O)]).

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, yokozuna),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, yokozuna_vnode_master).

install_postcommit(Bucket) when is_binary(Bucket) ->
    ModT = {<<"mod">>, <<"yokozuna">>},
    PostT = {<<"fun">>, <<"postcommit">>},
    Struct = {struct, [ModT, PostT]},
    riak_core_bucket:set_bucket(Bucket, [{postcommit, [Struct]}]).

%% TODO: This is tied to KV, not sure I want knowledge of KV in yokozuna?
%%
%% TODO: don't force 1:1 mapping of bucket to index
postcommit(RO) ->
    ReqId = erlang:phash2(erlang:now()),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {Bucket, _Key} = BKey = {riak_object:bucket(RO), riak_object:key(RO)},
    %% Hash same as KV (doc-based partitioning)
    Idx = riak_core_util:chash_key(BKey),
    BProps = riak_core_bucket:get_bucket(Bucket, Ring),
    Idx = riak_core_util:chash_key(BKey),
    NVal = riak_core_bucket:n_val(BProps),
    UpNodes = riak_core_node_watcher:nodes(?YZ_SVC_NAME),
    Preflist = riak_core_apl:get_apl(Idx, NVal, Ring, UpNodes),
    FPN = ?INT_TO_BIN(first_partition(Preflist)),
    Doc = yz_doc:make_doc(RO),
    Doc2 = yz_doc:add_to_doc(Doc, {'_fpn', FPN}),
    Index = Bucket,
    yokozuna_vnode:index(Preflist, binary_to_list(Index), Doc2, ReqId).

first_partition([{Partition, _}|_]) ->
    Partition.

search(Index, Query, Mapping) ->
    yz_solr:search(Index, [{q, Query}], Mapping).

solr_port(Node, Ports) ->
    proplists:get_value(Node, Ports).

%%%===================================================================
%%% Private
%%%===================================================================

test_it(Index) ->
    B = <<"fruit">>,
    O1 = riak_object:new(B, <<"apples">>, <<"2">>),
    O2 = riak_object:new(B, <<"oranges">>, <<"1">>),
    O3 = riak_object:new(B, <<"strawberries">>, <<"6">>),
    O4 = riak_object:new(B, <<"lemons">>, <<"1">>),
    O5 = riak_object:new(B, <<"celery">>, <<"4">>),
    O6 = riak_object:new(B, <<"lime">>, <<"1">>),
    [index(Index, O) || O <- [O1, O2, O3, O4, O5, O6]],
    yz_solr:commit(Index).

demo_write_objs(Index) ->
    ibrowse:start(),
    write_n_objs(Index, 1000),
    yz_solr:commit(Index).

demo_build_tree(Index, Name) ->
    ibrowse:start(),
    TP = yz_entropy:new_tree_proc(Index, Name),
    Pid = element(3, TP),
    Ref = make_ref(),
    Pid ! {get_tree, self(), Ref},
    receive {tree, Ref, Tree} -> Tree end,
    %% returning TreeProc too in case want to play with it
    {Tree, TP}.

demo_new_vclock(Index, N) ->
    %% the timestamp will change causing hash to change
    NS = list_to_binary(integer_to_list(N)),
    B = <<"test">>,
    K = <<"key_",NS/binary>>,
    V = <<"val_",NS/binary>>,
    O = riak_object:new(B, K, V),
    O2 = riak_object:increment_vclock(O, dummy_node),
    index(Index, O2),
    yz_solr:commit(Index).

demo_delete(Index, N) ->
    NS = integer_to_list(N),
    K = "key_" ++ NS,
    ok = yz_solr:delete(Index, {id,K}),
    ok = yz_solr:commit(Index).

write_n_objs(_, 0) ->
    ok;
write_n_objs(Index, N) ->
    NS = list_to_binary(integer_to_list(N)),
    B = <<"test">>,
    K = <<"key_",NS/binary>>,
    V = <<"val_",NS/binary>>,
    O = riak_object:new(B, K, V),
    O2 = riak_object:increment_vclock(O, dummy_node),
    index(Index, O2),
    write_n_objs(Index, N-1).
