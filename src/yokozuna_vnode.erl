-module(yokozuna_vnode).
-behaviour(riak_core_vnode).
-include("yokozuna.hrl").

-export([
         index/4,
         start_vnode/1
        ]).

-export([
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3
        ]).

-record(state, {partition, partition_binary}).
-define(PARTITION_BINARY(S), S#state.partition_binary).

%%%===================================================================
%%% API
%%%===================================================================

index(Preflist, Index, Doc, ReqId) ->
    Cmd = ?YZ_INDEX_CMD{doc=Doc, index=Index, req_id=ReqId},
    riak_core_vnode_master:command(Preflist, Cmd, ?YZ_VNODE_MASTER).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


%%%===================================================================
%%% Callbacks
%%%===================================================================

init([Partition]) ->
    PartitionBinary = ?INT_TO_BIN(Partition),
    {ok, #state {partition=Partition, partition_binary=PartitionBinary}}.

handle_command(?YZ_INDEX_CMD{doc=Doc, index=Index}, _Sender, State) ->
    Reply = handle_index_cmd(Index, Doc, ?PARTITION_BINARY(State)),
    {reply, Reply, State};

handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command(Message, _Sender, State) ->
    lager:error("unhandled command ~p", [Message]),
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% Private
%%%===================================================================

add_partition(Doc, Partition) ->
    yz_doc:add_to_doc(Doc, {'_pn', Partition}).

handle_index_cmd(Index, Doc, Partition) ->
    Doc2 = add_partition(Doc, Partition),
    yz_solr:index(Index, [Doc2]).
