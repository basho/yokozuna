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
%% -compile([export_all,{parse_transform,pulse_instrument},{d,modargs}]). %%TODO: Dynamically add pulse. NOT PRODUCTION
%% -compile({pulse_replace_module, [{gen_server, pulse_gen_server}]}).

-include("yokozuna.hrl").

%% api
-export([start_link/1, status/1, status/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% solrq/helper interface
-export([index_ready/3, index_batch/3]).

-type solr_ops()     :: [{add, {struct, [{atom(), binary()}]}} |
                         {delete, {struct, [{atom(), binary()}]}}].
-type solr_entries() :: [{bkey(), obj(), write_reason(), p(), short_preflist(),
                          hash()}].

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

status(Pid) ->
    status(Pid, 60000). % solr can block, long timeout by default

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

%% Mark the index as ready.  Separating into a two phase
%% rather than just blindly sending from the solrq adds the
%% backpressure on the KV vnode.
index_ready(HPid, Index, QPid) when is_atom(HPid); is_pid(HPid) ->
    gen_server:cast(HPid, {ready, Index, QPid});
index_ready(Hash, Index, QPid) ->
    HPid = yz_solrq_sup:helper_regname(Hash),
    index_ready(HPid, Index, QPid).

%% Send a batch
index_batch(HPid, Index, Entries) ->
    gen_server:cast(HPid, {batch, Index, Entries}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call(status, _From, #state{}) ->
    {reply, []};
handle_call(BadMsg, _From, State) ->
    {reply, {error, {unknown, BadMsg}}, State}.

handle_cast({ready, Index, QPid}, State) ->
    yz_solrq:request_batch(QPid, Index, self()),
    {noreply, State};
handle_cast({batch, Index, Entries0}, State) ->
    try
        %% TODO: use ibrowse http worker
        %% TODO: batch updates to YZ AAE
        %% TODO: move the owned/next partition logic back up
        %%       to yz_kv:index/3 once we efficiently cache
        %%       owned and next rather than calculating per-object.
        Ring = yz_misc:get_ring(transformed),
        LI = yz_cover:logical_index(Ring),
        OwnedAndNext = yz_misc:owned_and_next_partitions(node(), Ring),

        Entries = [{BKey, Obj, Reason, P,
                    riak_kv_util:get_index_n(BKey), yz_kv:hash_object(Obj)} ||
                      {BKey, Obj, Reason, P} <-
                          filter_out_fallbacks(OwnedAndNext, Entries0)],

        case update_solr(Index, LI, Entries) of
            ok ->
                update_aae_and_repair_stats(Entries);
            {error, Reason} ->
                ok
        end,
        {noreply, State}
    catch
        _:Err ->
            yz_stat:index_fail(),
            Trace = erlang:get_stacktrace(),
            ?ERROR("index ~p failed - ~p\nat: ~p", [Index, Err, Trace]),
            {noreply, State}
    end.

%% @doc Filter out all entries for partitions that are not currently owned or
%%      this node is a future owner of.
filter_out_fallbacks(OwnedAndNext, Entries) ->
    lists:filter(fun({_Bkey, _Obj, _Reason, P}) ->
                          ordsets:is_element(P, OwnedAndNext)
                 end, Entries).

%% Entries is [{Index, BKey, Obj, Reason, P, ShortPL, Hash}]
update_solr(_Index, _LI, []) -> % nothing left after filtering fallbacks
    ok;
update_solr(Index, LI, Entries) ->
    case yz_kv:should_index(Index) of
        false ->
            ok; % No need to send anything to SOLR, still need for AAE.
        _ ->
            IndexName = (?BIN_TO_ATOM(Index)),
            case yz_fuse:check(IndexName) of
                ok ->
                    send_solr_ops(Index, solr_ops(LI, Entries));
                blown ->
                    ?ERROR("Fuse Blown: can't current send solr "
                           "operations for index ~s", [Index]),
                    {error, fuse_blown};
                _ ->
                    %% fuse table creation is idempotent and occurs on
                    %% yz_index:add_index/1 on 1st creation or diff-check.
                    %% We send entries until we can ask again for
                    %% ok | error, as we wait for the tick.
                    send_solr_ops(Index, solr_ops(LI, Entries))
            end
    end.

%% Build the SOLR query
solr_ops(LI, Entries) ->
    lists:reverse(
      lists:foldl(
        fun({BKey, Obj0, Reason0, P, ShortPL, Hash}, Ops) ->
                {Bucket, _} = BKey,
                BProps = riak_core_bucket:get_bucket(Bucket),
                Obj = yz_kv:maybe_merge_siblings(BProps, Obj0),
                ObjValues = riak_object:get_values(Obj),
                Reason = get_reason_action(Reason0),
                case {Reason, ObjValues} of
                    {delete, _} ->
                        [{delete, yz_solr:encode_delete({bkey, BKey})} | Ops];
                    {_, [notfound]} ->
                        [{delete, yz_solr:encode_delete({bkey, BKey})} | Ops];
                    _ ->
                        LFPN = yz_cover:logical_partition(LI, element(1, ShortPL)),
                        LP = yz_cover:logical_partition(LI, P),
                        Docs = yz_doc:make_docs(Obj, Hash, ?INT_TO_BIN(LFPN), ?INT_TO_BIN(LP)),
                        AddOps = yz_doc:adding_docs(Docs),
                        DeleteOps = yz_kv:delete_operation(BProps, Obj, Reason, Docs, BKey, LP),
                        %% List will be reversed, so make sure deletes happen before adds
                        lists:append([[{add, yz_solr:encode_doc(Doc)} || Doc <- AddOps],
                                      [{delete, yz_solr:encode_delete(DeleteOp)} || DeleteOp <- DeleteOps],
                                      Ops])
                end
        end, [], Entries)).

-spec send_solr_ops(index_name(), solr_ops()) -> ok.
send_solr_ops(Index, Ops) ->
    try
        T1 = os:timestamp(),
        ok = yz_solr:index_batch(Index, Ops),
        yz_stat:index_end(Index, length(Ops), ?YZ_TIME_ELAPSED(T1))
    catch _:Err ->
            yz_stat:index_fail(),
            Trace = erlang:get_stacktrace(),
            ?ERROR("batch for index ~s failed - ~p\n with operations: ~p : ~p",
                   [Index, Err, Ops, Trace]),
            case Err of
                {_, badrequest, _} ->
                    send_solr_single_ops(Index, Ops),
                    ok;
                _ ->
                    yz_fuse:melt(Index),
                    {error, {Err, Trace}}
            end
    end.

%% @doc If solr batch fails on a `400' bad request, then retry individual ops
%%      in the batch they would have passed, logging a second error for the
%%      individual failure case(s), but ending with an ok to pass through.
-spec send_solr_single_ops(index_name(), solr_ops()) -> ok.
send_solr_single_ops(Index, Ops) ->
    lists:foreach(
      fun(Op) ->
              try
                  T1 = os:timestamp(),
                  ok = yz_solr:index_batch(Index, [Op]),
                  yz_stat:index_end(Index, length(Ops), ?YZ_TIME_ELAPSED(T1))
              catch _:Err ->
                      yz_stat:index_fail(),
                      Trace = erlang:get_stacktrace(),
                      ?ERROR("update for index ~s failed - ~p\n with operation: ~p : ~p",
                             [Index, Err, Op, Trace])
              end
      end, Ops).

-spec update_aae_and_repair_stats(solr_entries()) -> ok.
update_aae_and_repair_stats(Entries) ->
    Repairs = lists:foldl(
                fun({BKey, _Obj, Reason, P, ShortPL, Hash}, StatsD) ->
                        ReasonAction = get_reason_action(Reason),
                        Action = case ReasonAction of
                                     delete -> delete;
                                     _ -> {insert, Hash}
                                 end,
                        yz_kv:update_hashtree(Action, P, ShortPL, BKey),
                        gather_counts({P, ShortPL, Reason}, StatsD)
                end, dict:new(), Entries),
    dict:map(fun({Index, IndexN}, Count) ->
                  case Count of
                      0 ->
                          ok;
                      Count ->
                          lager:debug("Repaired ~b keys during active anti-entropy "
                                      "exchange of partition ~p for preflist ~p",
                                     [Count, Index, IndexN]),
                          yz_kv:update_aae_exchange_stats(Index, IndexN, Count)
                  end
             end, Repairs),
    ok.

-spec gather_counts({p(), {p(), n()}, write_reason()}, yz_dict()) -> yz_dict().
gather_counts({Index, IndexN, Reason}, StatsD) ->
    case Reason of
        {_, full_repair} ->
            dict:update_counter({Index, IndexN}, 1, StatsD);
        _ -> dict:update_counter({Index, IndexN}, 0, StatsD)
    end.

-spec get_reason_action(write_reason()) -> write_reason().
get_reason_action(Reason) when is_tuple(Reason) ->
    element(1, Reason);
get_reason_action(Reason) ->
    Reason.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
