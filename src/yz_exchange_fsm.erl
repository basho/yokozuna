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

    case yz_entropy_mgr:get_lock(?MODULE) of
        ok ->
            case yz_index_hashtree:get_lock(YZTree, ?MODULE) of
                ok ->
                    case riak_kv_entropy_manager:get_lock(?MODULE) of
                        ok ->
                            case riak_kv_index_hashtree:get_lock(KVTree,
                                                                 ?MODULE) of
                                ok ->
                                    update_trees(start_exchange, S);
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

update_trees(start_exchange, S=#state{yz_tree=YZTree,
                                      kv_tree=KVTree,
                                      index=Index,
                                      index_n=IndexN}) ->

    update_request(yz_index_hashtree, YZTree, Index, IndexN),
    update_request(riak_kv_index_hashtree, KVTree, Index, IndexN),
    {next_state, update_trees, S};

update_trees({not_responsible, Index, IndexN}, S) ->
    lager:debug("Index ~p does not cover preflist ~p", [Index, IndexN]),
    send_exchange_status({not_responsible, Index, IndexN}, S),
    {stop, normal, S};

update_trees({tree_built, _, _}, S) ->
    Built = S#state.built + 1,
    case Built of
        2 ->
            lager:debug("Moving to key exchange"),
            {next_state, key_exchange, S, 0};
        _ ->
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
                     exchange_segment_kv(KVTree, IndexN, Segment)
             end,

    AccFun = fun(KeyDiff, Acc) ->
                     lists:foldl(fun(Diff, Acc2) ->
                                         repair(Index, Diff),
                                         case Acc2 of
                                             [] -> [1];
                                             [Count] -> [Count+1]
                                         end
                                 end, Acc, KeyDiff)
             end,

    case yz_index_hashtree:compare(IndexN, Remote, AccFun, YZTree) of
        [] ->
            yz_kv:update_aae_exchange_stats(Index, IndexN, 0),
            ok;
        [Count] ->
            yz_kv:update_aae_exchange_stats(Index, IndexN, Count),
            lager:info("Repaired ~b keys during active anti-entropy exchange "
                       "of partition ~p for preflist ~p",
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
-spec repair(p(), keydiff()) -> ok.
repair(Partition, {remote_missing, KeyBin}) ->
    %% Yokozuna has it but KV doesn't
    Ring = yz_misc:get_ring(transformed),
    BKey = binary_to_term(KeyBin),
    Index = yz_kv:get_index(BKey, Ring),
    ShortPL = yz_kv:get_short_preflist(BKey, Ring),
    FakeObj = fake_kv_object(BKey),
    %% Repeat sopme logic in `yz_kv:index/3' to avoid extra work.  Can
    %% assume that Yokozuna is enabled and current node is owner.
    case yz_kv:should_index(Index) of
        true ->
            yz_kv:index(FakeObj, delete, Ring, Partition, BKey, ShortPL, Index),
            ok;
        false ->
            yz_kv:dont_index(FakeObj, delete, Partition, BKey, ShortPL)
    end,
    ok;
repair(Partition, {_Reason, KeyBin}) ->
    %% Either Yokozuna is missing the key or the hash doesn't
    %% match. In either case the object must be re-indexed.
    Ring = yz_misc:get_ring(transformed),
    BKey = binary_to_term(KeyBin),
    Index = yz_kv:get_index(BKey, Ring),
    ShortPL = yz_kv:get_short_preflist(BKey, Ring),
    %% Repeat some logic in `yz_kv:index/3` to avoid object get when
    %% possible.  Can assume here that Yokozua is enabled and current
    %% node is owner.
    case yz_kv:should_index(Index) of
        true ->
            case yz_kv:local_get(Partition, BKey) of
                {ok, Obj} ->
                    yz_kv:index(Obj, anti_entropy, Ring, Partition, BKey, ShortPL, Index),
                    ok;
                _Other ->
                    %% In most cases Other will be `{error, notfound}' which
                    %% is fine because hashtree updates are async and the
                    %% Yokozuna tree could see the delete before the KV tree.
                    %% That would cause exchange to trigger repair but then in
                    %% the meantime the KV object has since been deleted.  In
                    %% the case of other errors just ignore them and let the
                    %% next exchange retry the repair if it is still needed.
                    ok
            end;
        false ->
            yz_kv:dont_index(fake_kv_object(BKey), anti_entropy, Partition, BKey, ShortPL),
            ok
    end.

%% @private
fake_kv_object({Bucket, Key}) ->
    riak_object:new(Bucket, Key, <<"fake object">>).

%% @private
update_request(Module, Tree, Index, IndexN) ->
    as_event(fun() ->
                     case Module:update(IndexN, Tree) of
                         ok -> {tree_built, Index, IndexN};
                         not_responsible -> {not_responsible, Index, IndexN}
                     end
             end).

%% @private
as_event(F) ->
    Self = self(),
    spawn_link(fun() ->
                       Result = F(),
                       gen_fsm:send_event(Self, Result)
               end),
    ok.

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
