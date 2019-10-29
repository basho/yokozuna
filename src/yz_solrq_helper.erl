%% -------------------------------------------------------------------
%% Copyright (c) 2015 Basho Technologies, Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(yz_solrq_helper).

-include("yokozuna.hrl").
-include("stacktrace.hrl").

-behavior(gen_server).

%% api
-export([start_link/2, status/1, status/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% solrq/helper interface
-export([index_batch/5]).

%% TODO: Dynamically pulse_instrument.
-ifdef(EQC).
-define(EQC_DEBUG(S, F), _=element(1, {S, F}), ok).
%%-define(EQC_DEBUG(S, F), eqc:format(S, F)).
%%-define(EQC_DEBUG(S, F), io:fwrite(user, S, F)).
debug_entries(Entries) ->
    [erlang:element(1, Entry) || Entry <- Entries].
-else.
-define(EQC_DEBUG(S, F), ok).
-endif.

-record(state, {}).
-type solr_op()      :: {add, {struct, [{atom(), binary()}]}} |
                        {delete, {struct, [{atom(), binary()}]}}.
-type solr_op_list() :: [[solr_op()]].

-type write_action() :: put | delete | anti_entropy |
                        anti_entropy_delete | handoff.

%% Would look like:
%% Ops per Entry: [[adds], [deletes]]
%% All Ops: [[[adds], [deletes]]]
-type solr_ops()     :: [solr_op_list()].


%%%===================================================================
%%% API
%%%===================================================================

start_link(Index, Partition) ->
    gen_server:start_link({local, yz_solrq:helper_regname(Index, Partition)}, ?MODULE, [], []).

status(Pid) ->
    status(Pid, 60000). % solr can block, long timeout by default

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

%% @doc Index a batch
-spec index_batch(solrq_helper_id(),
                  index_name(),
                  BatchMax :: non_neg_integer(),
                  solrq_id(),
                  solr_entries()) -> ok.
index_batch(HPid, Index, BatchMax, QPid, Entries) ->
    gen_server:cast(HPid, {batch, Index, BatchMax, QPid, Entries}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call(status, _From, State) ->
    {reply, ok, State};
handle_call(BadMsg, _From, State) ->
    {reply, {error, {unknown, BadMsg}}, State}.

handle_cast({batch, Index, BatchMax, QPid, Entries}, State) ->
    ?EQC_DEBUG("Handling batch for index ~p.  Entries: ~p~n", [Index, debug_entries(Entries)]),
    Message = case do_batches(Index, BatchMax, [], Entries) of
        ok ->
            {length(Entries), ok};
        {ok, Delivered} ->
            {length(Delivered), {retry, remove(Delivered, Entries)}};
        {error, Undelivered} ->
            %% ?EQC_DEBUG("Error handling batch for index ~p.  Undelivered: ~p~n", [Index, debug_entries(Undelivered)]),
            {length(Entries) - length(Undelivered), {retry, Undelivered}}
    end,
    yz_solrq_worker:batch_complete(QPid, Message),
    {noreply, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec remove(solr_entries(), solr_entries()) -> solr_entries().
remove(Delivered, Entries) ->
    %% TODO Performance stinks, but this will only be used in the (hopefully) degenerate
    %% of having to handle bad requests.
    Entries -- Delivered.

-spec do_batches(index_name(),
                 BatchMax :: non_neg_integer(),
                 solr_entries(),
                 solr_entries()) ->
    ok |
    {ok, Delivered :: solr_entries()} |
    {error, Undelivered :: solr_entries()}.
do_batches(_Index, _BatchMax, _Delivered, []) ->
    ok;
do_batches(Index, BatchMax, Delivered, Entries) ->
    {Entries1, Rest} = lists:split(min(length(Entries), BatchMax), Entries),
    case do_batch(Index, Entries1) of
        ok ->
            do_batches(Index, BatchMax, Delivered ++ Entries1, Rest);
        {ok, DeliveredInBatch} ->
            {ok, DeliveredInBatch ++ Delivered};
        {error, _Reason} ->
            %% ?EQC_DEBUG("Error handling batch:~p", [_Reason]),
            {error, Entries}
    end.

-spec do_batch(index_name(), solr_entries()) ->
      ok                                        % all entries were delivered
    | {ok, Delivered :: solr_entries()}         % a strict subset of entries were delivered
    | {error, Reason :: term()}.                % an error occurred; retry all of them
do_batch(Index, Entries0) ->
    %% TODO: use ibrowse http worker
    %% TODO: batch updates to YZ AAE
    %% TODO: move the owned/next partition logic back up
    %%       to yz_kv:index/3 once we efficiently cache
    %%       owned and next rather than calculating per-object.
    Ring = yz_misc:get_ring(transformed),
    LI = yz_cover:logical_index(Ring),
    OwnedAndNext = yz_misc:owned_and_next_partitions(node(), Ring),

    Entries1 = [{BKey, {Obj, _OldObj}, Reason, P,
                 riak_kv_util:get_index_n(BKey), yz_kv:hash_object(Obj, P)} ||
        {BKey, {Obj, _OldObj}, Reason, P} <-
            yz_misc:filter_out_fallbacks(OwnedAndNext, Entries0)],
    case update_solr(Index, LI, Entries1) of
        ok ->
            update_aae_and_repair_stats(Entries1),
            ok;
        {ok, Entries2} ->
            update_aae_and_repair_stats(Entries2),
            {ok, [{BKey, Objects, Reason, P} || {BKey, Objects, Reason, P, _, _} <- Entries2]};
        {error, Reason} ->
            {error, Reason}
    end.


%% @doc Entries is [{BKey, Obj, Reason, P, ShortPL, Hash}]
-spec update_solr(index_name(), logical_idx(), solr_entries()) ->
                         ok | {ok, SuccessEntries :: solr_entries()} |
                         {error, fuse_blown} | {error, tuple()}.
update_solr(_Index, _LI, []) -> % nothing left after filtering fallbacks
    ok;
update_solr(Index, LI, Entries0) when ?YZ_SHOULD_INDEX(Index) ->
    case yz_fuse:check(Index) of
        blown ->
            %% ?EQC_DEBUG(                       "Fuse Blown: can't currently send solr "
            %%       "operations for index ~s", [Index]),
            {error, fuse_blown};
        _ ->
            %% fuse table creation is idempotent and occurs on
            %% yz_index:add_index/1 on 1st creation or diff-check.
            %% We send entries until we can ask again for
            %% ok | error, as we wait for the tick.
            Ops = solr_ops(LI, Entries0),
            send_solr_ops_for_entries(Index, Ops, Entries0)
    end;
update_solr(_Index, _LI, _Entries) ->
    ok.

%% @doc Build the SOLR query
-spec solr_ops(logical_idx(), solr_entries()) -> solr_ops().
solr_ops(LI, Entries) ->
    [get_ops_for_entry(Entry, LI) || Entry <- Entries].

-spec get_ops_for_entry(solr_entry(), logical_idx()) -> solr_ops().
get_ops_for_entry({BKey, {Obj0, _OldObj}=Objects, Reason, P, ShortPL, Hash}, LI) ->
    {Bucket, _} = BKey,
    BProps = riak_core_bucket:get_bucket(Bucket),
    Obj = yz_kv:maybe_merge_siblings(BProps, Obj0),
    ObjValues = riak_object:get_values(Obj),
    Action = get_reason_action(Reason),
    get_ops_for_entry_action(Action, ObjValues, LI, P, Objects, BKey, ShortPL,
        Hash, BProps).

-spec get_ops_for_entry_action(write_action(), [riak_object:value()],
        logical_idx(), p(), object_pair(), bkey(), short_preflist(), hash(),
        riak_core_bucket:properties()) -> solr_ops().
get_ops_for_entry_action(_Action, [notfound], LI, P, _Objects, BKey,
                         _ShortPL, _Hash, _BProps) ->
    LP = yz_cover:logical_partition(LI, P),
    [{delete, yz_solr:encode_delete({bkey, BKey, LP})}];
get_ops_for_entry_action(anti_entropy_delete, _ObjValues, LI, P, _FakeObjects, BKey,
                         _ShortPL, _Hash, _BProps) ->
    %% anti-entropy is the "case of last resort" and at this point
    %% we need to do a cleanup of _any_ documents that may be
    %% floating around.
    get_ops_for_object_cleanup(BKey, LI, P);
get_ops_for_entry_action(anti_entropy, _ObjValues, LI, P, {Obj, _OldObj}, BKey,
                         ShortPL, Hash, _BProps) ->
    %% anti-entropy is the "case of last resort" and at this point
    %% we need to do a cleanup of _any_ documents that may be
    %% floating around.
    DeleteOpsForEntry = get_ops_for_object_cleanup(BKey, LI, P),
    AddOpsForEntry = get_ops_for_add(LI, ShortPL, P, Obj, Hash),
    [DeleteOpsForEntry, AddOpsForEntry];
get_ops_for_entry_action(delete, _ObjValues, LI, P, {Obj, _OldObj}, _BKey,
                         _ShortPL, _Hash, BProps) ->
    [get_ops_for_deletes(LI, P, Obj, BProps)];

get_ops_for_entry_action(Action, _ObjValues, LI, P, {Obj, OldObj}, _BKey,
                         ShortPL, Hash, BProps) when
                                Action == handoff;
                                Action == put ->
    DeleteOps = get_ops_for_deletes(LI, P, OldObj, BProps),
    AddOps = get_ops_for_add(LI, ShortPL, P, Obj, Hash),
    [DeleteOps, AddOps].

get_ops_for_object_cleanup(BKey, LI, P) ->
    LP = yz_cover:logical_partition(LI, P),
    [[{delete, yz_solr:encode_delete({bkey, BKey, LP})}]].

get_ops_for_add(LI, ShortPL, P, Obj, Hash) ->
    LFPN = yz_cover:logical_partition(LI, element(1, ShortPL)),
    LP = yz_cover:logical_partition(LI, P),
    Docs = yz_doc:make_docs(Obj, Hash, ?INT_TO_BIN(LFPN),
                            ?INT_TO_BIN(LP)),
    AddOps = yz_doc:adding_docs(Docs),
    [{add, yz_solr:encode_doc(Doc)} ||
        Doc <- AddOps].


get_ops_for_deletes(_LI, _P, no_old_object, _BProps) ->
    [];
get_ops_for_deletes(LI, P, Obj, BProps) ->
    case yz_kv:siblings_permitted(Obj, BProps) of
        true ->
            get_ops_for_sibling_deletes(LI, P, Obj);
        _ ->
            get_ops_for_no_sibling_deletes(LI, P, Obj)
    end.

get_ops_for_no_sibling_deletes(LI, P, Obj) ->
    LP = yz_cover:logical_partition(LI, P),
    DocId = yz_doc:doc_id(Obj, ?INT_TO_BIN(LP)),
    [{delete, yz_solr:encode_delete({id, DocId})}].

get_ops_for_sibling_deletes(LI, P, Obj) ->
    LP = yz_cover:logical_partition(LI, P),
    DocIds = yz_doc:doc_ids(Obj, ?INT_TO_BIN(LP)),
    DeleteOps = [{delete, yz_solr:encode_delete({id, DocId})}
                    || DocId <- DocIds],
    [DeleteOps].

%% @doc A function that takes in an `Index', a list of `Ops' and the list
%%      of `Entries', and attempts to batch_index them into Solr.
%%
%%      If a `badrequest' is given, we attempt to apply each operation
%%      individually until it completes all ops (bypassing the badrequest and
%%      allowing it to update to tree to prevent forever repair) or it reaches
%%      a Solr Internal error of sorts, for which we stop list-processing and
%%      use the success-length to segment the entries list for `AAE-updates'.
-spec send_solr_ops_for_entries(index_name(), solr_ops(), solr_entries()) ->
                                       {ok, SuccessEntries :: solr_entries()} |
                                       {error, tuple()}.
send_solr_ops_for_entries(Index, Ops, Entries) ->
    T1 = os:timestamp(),
    PreparedOps = prepare_ops_for_batch(Index, Ops),
    %% ?EQC_DEBUG("send_solr_ops_for_entries: About to send entries. ~p", Entries),
    case yz_solr:index_batch(Index, PreparedOps) of
        ok ->
            yz_stat:index_end(Index, length(Ops), ?YZ_TIME_ELAPSED(T1)),
            ok;
        {error, {Reason, _Detail}} = Err when Reason =:= badrequest; Reason =:= bad_data ->
            ?DEBUG("batch for index ~s failed.  Error: ~p~n", [Index, Err]),
            %yz_stat:index_fail(),
            handle_bad_entries(Index, Ops, Entries);
        Err ->
            ?DEBUG("batch for index ~s failed.  Error: ~p~n", [Index, Err]),
            ?ERROR("Updating a batch of Solr operations failed for index ~p with error ~p", [Index, Err]),
            yz_fuse:melt(Index),
            Trace = ?_current_stacktrace_(),
            {error, {Err, Trace}}
    end.

handle_bad_entries(Index, Ops, Entries) ->
    SuccessOps = send_solr_single_ops(Index, Ops),
    {SuccessEntries, _} = lists:split(length(SuccessOps), Entries),
    {ok, SuccessEntries}.

%% @doc If solr batch fails on a `400' bad request, then retry individual ops
%%      in the batch that would/should have passed.
%%
%%      Allow the `badrequest' op to go through and be added to the AAE tree,
%%      so that we're not constantly repairing a bad document/val.
%%
%%      We only take and return operations that have either been indexed
%%      or get the badrequest. If we run into an internal Solr error, we
%%      `melt' the fuse once for the search_index and stop gathering
%%      successful ops and applying side-effects to Solr.
-spec send_solr_single_ops(index_name(), solr_ops()) -> GoodOps :: solr_ops().
send_solr_single_ops(Index, Ops) ->
    lists:takewhile(fun(Op) ->
                      single_op_batch(Index, Op)
                  end,
                  Ops).

single_op_batch(Index, Op) ->
    Ops = prepare_ops_for_batch(Index, [Op]),
    case yz_solr:index_batch(Index, Ops) of
        ok ->
            T1 = os:timestamp(),
            yz_stat:index_end(Index, length(Ops), ?YZ_TIME_ELAPSED(T1)),
            true;
        %% TODO This results in double counting index failures when
        %% we get a bad request back from Solr.
        %% We should probably refine our stats so that
        %% they differentiate between bad data and Solr going wonky
        {error, {Reason, _Details}} when Reason =:= badrequest; Reason =:= bad_data ->
            yz_stat:index_bad_entry(),
            ?ERROR("Updating a single Solr operation failed for index ~p with bad request.", [Index]),
            true;
        Err ->
            yz_stat:index_fail(),
            ?ERROR("Updating a single Solr operation failed for index ~p with error ~p", [Index, Err]),
            yz_fuse:melt(Index),
            false
    end.

-spec update_aae_and_repair_stats(solr_entries()) -> ok.
update_aae_and_repair_stats(Entries) ->
    update_hashtree_for_entries(Entries),
    update_repair_stats(Entries).

-spec update_repair_stats(solr_entries()) -> ok.
update_repair_stats(Entries) ->
    Repairs = calculate_repair_counts(Entries),
    update_stats(Repairs).

-spec calculate_repair_counts(solr_entries()) -> yz_dict().
calculate_repair_counts(Entries) ->
    Repairs = lists:foldl(fun increment_repair_count/2, dict:new(), Entries),
    Repairs.

-spec update_hashtree_for_entries(solr_entries()) -> ok.
update_hashtree_for_entries(Entries) ->
    lists:foreach(fun update_hashtree_for_entry/1, Entries).

-spec update_stats(yz_dict()) -> ok.
update_stats(RepairsDict) ->
    Repairs = dict:to_list(RepairsDict),
    lists:foreach(fun update_stat/1, Repairs).

-spec update_stat({{p(), short_preflist()}, non_neg_integer()}) -> ok.
update_stat({_, 0}) ->
    ok;
update_stat({{Partition, ShortPL}, Count}) ->
    lager:debug("Repaired ~b keys during active anti-entropy "
                "exchange of partition ~p for preflist ~p",
                [Count, Partition, ShortPL]),
    yz_kv:update_aae_exchange_stats(Partition, ShortPL, Count).

increment_repair_count({_BKey, _Obj, Reason, P, ShortPL, _Hash}, StatsD) ->
    maybe_add_repair_to_counts({P, ShortPL, Reason}, StatsD).

update_hashtree_for_entry({BKey, _Obj, Reason, P, ShortPL, Hash}) ->
    ReasonAction = get_reason_action(Reason),
    Action = hashtree_action(ReasonAction, Hash),
    yz_kv:update_hashtree(Action, P, ShortPL, BKey).

-spec hashtree_action(write_action(), hash()) ->
    delete | {insert, hash()}.
hashtree_action(delete, _Hash) ->
    delete;
hashtree_action(anti_entropy_delete, _Hash) ->
    delete;
hashtree_action(Action, Hash) when Action == put;
                                   Action == handoff;
                                   Action == anti_entropy ->
    {insert, Hash}.

-spec maybe_add_repair_to_counts({p(), short_preflist(), write_reason()}, yz_dict()) -> yz_dict().
maybe_add_repair_to_counts({Index, ShortPL, {_, full_repair}}, StatsD) ->
    dict:update_counter({Index, ShortPL}, 1, StatsD);
maybe_add_repair_to_counts({_Index, _ShortPL, _Reason}, StatsD) ->
    StatsD.

-spec get_reason_action(write_reason()) -> write_action().
get_reason_action(Reason) when is_tuple(Reason) ->
    element(1, Reason);
get_reason_action(Reason) ->
    Reason.

-define(QUERY(Bin), {struct, [{'query', Bin}]}).

-spec prepare_ops_for_batch(index_name(), solr_ops()) -> solr_entries().
prepare_ops_for_batch(_Index, Ops) ->
    %% Flatten combined operators for a batch.
    lists:flatten(Ops).

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.