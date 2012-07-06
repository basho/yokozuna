-module(yz_events).
-compile(export_all).
-behavior(gen_server).
-include("yokozuna.hrl").

%% @doc Functionality related to events.  This is the single producer of
%% writes to the ETS table `yz_events`.

%%%===================================================================
%%% API
%%%===================================================================

-spec get_mapping() -> list().
get_mapping() ->
    case ets:lookup(?YZ_EVENTS_TAB, mapping) of
        [{mapping, Mapping}] -> Mapping;
        [] -> []
    end.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init([]) ->
    ok = watch_ring_events(),
    ok = watch_node_events(),
    ok = create_events_table(),
    {ok, none}.

handle_cast({node_event, Node, Status}, S) ->
    Mapping = get_mapping(),
    Mapping2 = handle_node_event(Node, Status, Mapping),
    io:format("node event ~p ~p ~p~n", [Node, Status, Mapping2]),
    ok = set_mapping(Mapping2),
    {noreply, S};

handle_cast({ring_event, Ring}, S) ->
    io:format("ring event~n"),
    Mapping = get_mapping(),
    Mapping2 = handle_ring_event(Ring, Mapping),
    ok = set_mapping(Mapping2),
    {noreply, S}.


%%%===================================================================
%%% Private
%%%===================================================================

-spec add_node(node(), list()) -> list().
add_node(Node, Mapping) ->
    HostPort = host_port(Node),
    lists:keystore(Node, 1, Mapping, {Node, HostPort}).

-spec add_nodes([node()], list()) -> list().
add_nodes(Nodes, Mapping) ->
    lists:foldl(fun add_node/2, Mapping, Nodes).

-spec check_unkown(list()) -> list().
check_unkown(Mapping) ->
    Unknown = lists:filter(fun is_unknown/1, Mapping),
    add_nodes(just_nodes(Unknown), Mapping).

-spec create_events_table() -> ok.
create_events_table() ->
    Opts = [named_table, protected, {read_concurrency, true}],
    ?YZ_EVENTS_TAB = ets:new(?YZ_EVENTS_TAB, Opts),
    ok.

-spec handle_node_event(node(), up | down, list()) -> list().
handle_node_event(Node, down, Mapping) ->
    remove_node(Node, Mapping);
handle_node_event(Node, up, Mapping) ->
    add_node(Node, Mapping).

-spec handle_ring_event(riak_core_ring:ring(), list()) -> list().
handle_ring_event(Ring, Mapping) ->
    Nodes = riak_core_ring:all_members(Ring),
    {Removed, Added} = node_ops(Mapping, Nodes),
    Mapping2 = remove_nodes(Removed, Mapping),
    Mapping3 = add_nodes(Added, Mapping2),
    check_unkown(Mapping3).

-spec host_port(node()) -> {string(), non_neg_integer() | unknown}.
host_port(Node) ->
    case rpc:call(Node, yz_solr, port, [], 5000) of
        {badrpc, Reason} ->
            ?ERROR("error retrieving Solr port ~p ~p", [Node, Reason]),
            {hostname(Node), unknown};
        Port when is_list(Port) ->
            {hostname(Node), Port}
    end.

-spec hostname(node()) -> string().
hostname(Node) ->
    S = atom_to_list(Node),
    [_, Host] = re:split(S, "@", [{return, list}]),
    Host.

-spec is_unknown(tuple()) -> boolean().
is_unknown({_, {_, unknown}}) -> true;
is_unknown({_, {_, Port}}) when is_list(Port) -> false.

-spec just_nodes(list()) -> [node()].
just_nodes(Mapping) ->
    [Node || {Node, _} <- Mapping].

-spec node_ops(list(), list()) -> {Removed::list(), Added::list()}.
node_ops(Mapping, Nodes) ->
    MappingNodesSet = sets:from_list(just_nodes(Mapping)),
    NodesSet = sets:from_list(Nodes),
    Removed = sets:subtract(MappingNodesSet, NodesSet),
    Added = sets:subtract(NodesSet, MappingNodesSet),
    {sets:to_list(Removed), sets:to_list(Added)}.

-spec remove_node(node(), list()) -> list().
remove_node(Node, Mapping) ->
    proplists:delete(Node, Mapping).

-spec remove_nodes([node()], list()) -> list().
remove_nodes(Nodes, Mapping) ->
    lists:foldl(fun remove_node/2, Mapping, Nodes).

send_node_update({node_update, Node, Status}) ->
    gen_server:cast(?MODULE, {node_event, Node, Status});
send_node_update(_) ->
    ok.

send_ring_event(Ring) ->
    gen_server:cast(?MODULE, {ring_event, Ring}).

set_mapping(Mapping) ->
    true = ets:insert(?YZ_EVENTS_TAB, [{mapping, Mapping}]),
    ok.

watch_node_events() ->
    riak_core_node_watcher_events:add_sup_callback(fun send_node_update/1).

watch_ring_events() ->
    riak_core_ring_events:add_sup_callback(fun send_ring_event/1).

