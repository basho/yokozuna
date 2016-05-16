%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(yz_exchange_fsm).
-behaviour(gen_fsm).
-include("yokozuna.hrl").
-compile(export_all).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-record(state, {index :: p(),
                index_n :: short_preflist(),
                yz_tree :: tree(),
                kv_tree :: tree(),
                built :: integer(),
                timeout :: pos_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Initialize the exchange FSM to exchange between Yokozuna and
%%      KV for the `Preflist' replicas on `Index'.
-spec start(p(), {p(),n()}, tree(), tree(), pid()) ->
                   {ok, pid()} | {error, any()}.
start(Index, Preflist, YZTree, KVTree, Manager) ->
    gen_fsm:start(?MODULE, [Index, Preflist, YZTree, KVTree, Manager], []).

%% @doc Send this FSM a drain_error message, with the supplied reason.  Calling
%% drain error while the FSM is in the update_trees state will cause the FSM to stop
%%
%% IMPORTANT: This API call is really only intended to be used from the yz_solrq_drain_mgr
%% Use at your own risk (or ignore).
%% @end
%%
-spec drain_error(pid(), Reason::term()) -> ok.
drain_error(Pid, Reason) ->
    gen_fsm:send_event(Pid, {drain_error, Reason}).

%% @doc  Update the yz index hashtree.
%%
%% IMPORTANT: This API call is really only intended to be used from the yz_solrq_drain_fsm
%% Use at your own risk (or ignore).
%% @end
%%
-spec update_yz_index_hashtree(pid(), tree(), p(), short_preflist()) -> ok.
update_yz_index_hashtree(Pid, YZTree, Index, IndexN) ->
    do_update(Pid, yz_index_hashtree, YZTree, Index, IndexN).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([Index, Preflist, YZTree, KVTree, Manager]) ->
    monitor(process, Manager),
    monitor(process, YZTree),
    monitor(process, KVTree),

    S = #state{index=Index,
               index_n=Preflist,
               yz_tree=YZTree,
               kv_tree=KVTree,
               built=0,
               timeout=?YZ_ENTROPY_TIMEOUT},
    gen_fsm:send_event(self(), start_exchange),
    lager:debug("Starting exchange between KV and Yokozuna: ~p", [Index]),
    {ok, prepare_exchange, S}.

handle_event(_Event, StateName, S) ->
    {next_state, StateName, S}.

handle_sync_event(_Event, _From, StateName, S) ->
    {reply, ok, StateName, S}.

handle_info({'DOWN', _, _, _, _}, _StateName, S) ->
    %% Either the entropy manager, local hashtree, or remote hashtree has
    %% exited. Stop exchange.
    {stop, normal, S};

handle_info(_Info, StateName, S) ->
    {next_state, StateName, S}.

terminate(_Reason, _StateName, _S) ->
    ok.

code_change(_OldVsn, StateName, S, _Extra) ->
    {ok, StateName, S}.

%%%===================================================================
%%% States
%%%===================================================================

prepare_exchange(start_exchange, S) ->
    YZTree = S#state.yz_tree,
    KVTree = S#state.kv_tree,

    try
        ok = yz_entropy_mgr:get_lock(?MODULE),
        ok = yz_index_hashtree:get_lock(YZTree, ?MODULE),
        ok = riak_kv_entropy_manager:get_lock(?MODULE),
        ok = riak_kv_index_hashtree:get_lock(KVTree, ?MODULE),
        update_trees(start_exchange, S)
    catch
        error:{badmatch, Reason} ->
            send_exchange_status(Reason, S),
            {stop, normal, S}
    end;

prepare_exchange(timeout, S) ->
    do_timeout(S).

update_trees(start_exchange, S=#state{kv_tree=KVTree,
                                      yz_tree = YZTree,
                                      index=Index,
                                      index_n=IndexN}) ->
    Self = self(),
    update_request(
        riak_kv_index_hashtree, KVTree, Index, IndexN,
        fun() ->
            yz_solrq_drain_mgr:drain([
                {?EXCHANGE_FSM_PID, Self},
                {?YZ_INDEX_HASHTREE_PARAMS, {YZTree, Index, IndexN}},
                {?DRAIN_PARTITION, Index}
            ])
        end
    ),
    {next_state, update_trees, S};

update_trees({drain_error, Reason}, S) ->
    lager:error("Drain failed with reason ~p", [Reason]),
    send_exchange_status(drain_failed, S),
    {stop, normal, S};

update_trees({not_responsible, Index, IndexN}, S) ->
    lager:debug("Index ~p does not cover preflist ~p", [Index, IndexN]),
    send_exchange_status({not_responsible, Index, IndexN}, S),
    {stop, normal, S};

update_trees({tree_built, Module, _, _}, S) ->
    Built = S#state.built + 1,
    case Built of
        2 ->
            lager:debug("Tree ~p built; Moving to key exchange", [Module]),
            {next_state, key_exchange, S, 0};
        _ ->
            lager:debug("Tree ~p built; staying in update_trees state", [Module]),
            {next_state, update_trees, S#state{built=Built}}
    end.

key_exchange(timeout, S=#state{index=Index,
                               yz_tree=YZTree,
                               kv_tree=KVTree,
                               index_n=IndexN}) ->
    lager:debug("Starting key exchange for partition ~p preflist ~p",
                [Index, IndexN]),

    Remote = fun(get_bucket, {L, B}) ->
                     exchange_bucket_kv(KVTree, IndexN, L, B);
                (key_hashes, Segment) ->
                     exchange_segment_kv(KVTree, IndexN, Segment);
                (_, _) ->
                     ok
             end,

    AccFun = fun(KeyDiff, Count) ->
                     lists:foldl(fun(Diff, InnerCount) ->
                                     case repair(Index, Diff) of
                                         full_repair -> InnerCount + 1;
                                         _ -> InnerCount
                                     end
                                 end, Count, KeyDiff)
             end,
    case yz_index_hashtree:compare(IndexN, Remote, AccFun, 0, YZTree) of
        0 ->
            yz_kv:update_aae_exchange_stats(Index, IndexN, 0);
        Count ->
            yz_stat:detected_repairs(Count),
            lager:info("Will repair ~b keys of partition ~p for preflist ~p",
                       [Count, Index, IndexN])
    end,
    {stop, normal, S}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
exchange_bucket_kv(Tree, IndexN, Level, Bucket) ->
    riak_kv_index_hashtree:exchange_bucket(IndexN, Level, Bucket, Tree).

%% @private
exchange_segment_kv(Tree, IndexN, Segment) ->
    riak_kv_index_hashtree:exchange_segment(IndexN, Segment, Tree).

%% @private
%%
%% @doc If Yokozuna gets {remote_missing, _} b/c yz has it, but kv doesn't.
%%      If Yokozuna gets any other repair, then it's either b/c
%%      yz is missing the key or the hash doesn't match. For those cases,
%%      we must reindex.
-spec repair(p(), keydiff()) -> repair().
repair(Partition, {remote_missing, KeyBin}) ->
    BKey = binary_to_term(KeyBin),
    Index = yz_kv:get_index(BKey),
    FakeObj = fake_kv_object(BKey),
    case yz_kv:should_index(Index) of
        true ->
            Repair = full_repair,
            yz_solrq:index(Index, BKey, FakeObj, {anti_entropy_delete, Repair}, Partition),
            Repair;
        false ->
            Repair = tree_repair,
            yz_solrq:index(Index, BKey, FakeObj, {anti_entropy_delete, Repair}, Partition),
            Repair
    end;
repair(Partition, {_Reason, KeyBin}) ->
    %% Either Yokozuna is missing the key or the hash doesn't
    %% match. In either case the object must be re-indexed.
    BKey = binary_to_term(KeyBin),
    Index = yz_kv:get_index(BKey),
    %% Can assume here that Yokozua is enabled and current
    %% node is owner.
    case yz_kv:local_get(Partition, BKey) of
        {ok, Obj} ->
            case yz_kv:should_index(Index) of
                true ->
                    Repair = full_repair,
                    yz_solrq:index(Index, BKey, Obj, {anti_entropy, Repair},
                                   Partition),
                    Repair;
                false ->
                    Repair = tree_repair,
                    yz_solrq:index(Index, BKey, Obj, {anti_entropy, Repair},
                                   Partition),
                    Repair
            end;
        _Other ->
            %% In most cases Other will be `{error, notfound}' which
            %% is fine because hashtree updates are async and the
            %% Yokozuna tree could see the delete before the KV tree.
            %% That would cause exchange to trigger repair but then in
            %% the meantime the KV object has since been deleted.  In
            %% the case of other errors just ignore them and let the
            %% next exchange retry the repair if it is still needed.
            failed_repair
    end.

%% @private
fake_kv_object({Bucket, Key}) ->
    riak_object:new(Bucket, Key, <<"fake object">>).

%% @private
do_update(ToWhom, Module, Tree, Index, IndexN) ->
    do_update(ToWhom, Module, Tree, Index, IndexN, undefined).

%% @private
do_update(ToWhom, Module, Tree, Index, IndexN, Callback) ->
    UpdateResult = module_update(Module, IndexN, Tree, Callback),
    Result = case UpdateResult of
                 ok -> {tree_built, Module, Index, IndexN};
                 not_responsible -> {not_responsible, Index, IndexN}
             end,
    gen_fsm:send_event(ToWhom, Result),
    UpdateResult.

%% @private
module_update(riak_kv_index_hashtree, Index, Tree, Callback) ->
    riak_kv_index_hashtree:update(Index, Tree, Callback);
module_update(yz_index_hashtree, Index, Tree, undefined) ->
    yz_index_hashtree:update(Index, Tree).

%% @private
update_request(Module, Tree, Index, IndexN, Callback) ->
    spawn_update_request(self(), Module, Tree, Index, IndexN, Callback).


%% @private
spawn_update_request(ToWhom, Module, Tree, Index, IndexN, Callback) ->
    spawn_link(?MODULE, do_update, [ToWhom, Module, Tree, Index, IndexN, Callback]).

%% @private
do_timeout(S=#state{index=Index, index_n=Preflist}) ->
    lager:info("Timeout during exchange of partition ~p for preflist ~p ",
               [Index, Preflist]),
    send_exchange_status({timeout, Index, Preflist}, S),
    {stop, normal, S}.

%% @private
send_exchange_status(Status, #state{index=Index,
                                    index_n=IndexN}) ->
    yz_entropy_mgr:exchange_status(Index, IndexN, Status).
