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
                index_n :: {p(),n()},
                yz_tree :: tree(),
                kv_tree :: tree(),
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

    case yz_entropy_mgr:get_lock(?MODULE) of
        ok ->
            case yz_index_hashtree:get_lock(YZTree, ?MODULE) of
                ok ->
                    case riak_kv_entropy_manager:get_lock(?MODULE) of
                        ok ->
                            case riak_kv_index_hashtree:get_lock(KVTree,
                                                                 ?MODULE) of
                                ok ->
                                    %case yz_solrq_drain_mgr:get_lock() of
                                    %    ok ->
                                            update_trees(start_exchange, S);
                                    %    _ ->
                                    %        send_exchange_status(drain_in_progress, S),
                                    %        {stop, normal, S}
                                    %end;
                                _ ->
                                    send_exchange_status(already_locked, S),
                                    {stop, normal, S}
                            end;
                        Error ->
                            send_exchange_status(Error, S),
                            {stop, normal, S}
                    end;
                _ ->
                    send_exchange_status(already_locked, S),
                    {stop, normal, S}
            end;
        Error ->
            send_exchange_status(Error, S),
            {stop, normal, S}
    end;

prepare_exchange(timeout, S) ->
    do_timeout(S).

update_trees(start_exchange, S=#state{kv_tree=KVTree,
                                      index=Index,
                                      index_n=IndexN}) ->

    update_request(riak_kv_index_hashtree, KVTree, Index, IndexN),
    {next_state, update_trees, S};

update_trees({not_responsible, Index, IndexN}, S) ->
    lager:debug("Index ~p does not cover preflist ~p", [Index, IndexN]),
    send_exchange_status({not_responsible, Index, IndexN}, S),
    {stop, normal, S};

update_trees({tree_built, riak_kv_index_hashtree, _, _}, #state{yz_tree = YZTree, index = Index, index_n = IndexN} = S) ->
    case yz_solrq_drain_mgr:drain() of
        ok ->
            update_request(yz_index_hashtree, YZTree, Index, IndexN),
            {next_state, update_trees, S};
        %{error, timeout} ->
        %    lager:warning("A drain operation timed out during AAE exchange.  Consider increasing the yokozuna drain_timeout configuration property."),
        %    update_request(yz_index_hashtree, YZTree, Index, IndexN),
        %    {next_state, update_trees, S};
        {error, Reason} ->
            lager:error("A drain operation failed during AAE exchange.  Reason: ~p", [Reason]),
            send_exchange_status(drain_failed, S),
            {stop, normal, S}
    end;

update_trees({tree_built, yz_index_hashtree, _, _}, S) ->
    lager:debug("Moving to key exchange"),
    {next_state, key_exchange, S, 0}.

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
            yz_stat:aae_repairs(0),
            yz_kv:update_aae_exchange_stats(Index, IndexN, 0);
        Count ->
            yz_stat:aae_repairs(Count),
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
            yz_solrq:index(Index, BKey, FakeObj, {delete, Repair}, Partition),
            Repair;
        false ->
            Repair = tree_repair,
            yz_solrq:index(Index, BKey, FakeObj, {delete, Repair}, Partition),
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
    Result = case Module:update(IndexN, Tree) of
                 ok -> {tree_built, Module, Index, IndexN};
                 not_responsible -> {not_responsible, Index, IndexN}
             end,
    gen_fsm:send_event(ToWhom, Result).

%% @private
update_request(Module, Tree, Index, IndexN) ->
    update_request(self(), Module, Tree, Index, IndexN).


%% @private
update_request(ToWhom, Module, Tree, Index, IndexN) ->
    spawn_link(?MODULE, do_update, [ToWhom, Module, Tree, Index, IndexN]).

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
