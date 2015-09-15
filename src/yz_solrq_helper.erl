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
-compile([export_all]). % TODO: Replace with proper exports
-include("yokozuna.hrl").

%% api
-export([start_link/1, status/1, status/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% solrq/helper interface
-export([index_ready/3, index_batch/3]).

-record(state, {qpid,
                http_pid}).

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
    HPid = yz_solrq_helper_sup:regname(Hash),
    index_ready(HPid, Index, QPid).

%% Send a batch
index_batch(HPid, Index, Entries) ->
    gen_server:cast(HPid, {batch, Index, Entries}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    %% TODO: Prolly need to monitor
    %% Host = "localhost",
    %% Port = yz_solr:port(),
    %% {ok, HttpPid} = ibrowse_http_client:start({Host, Port}),
    {ok, #state{}}.

handle_call(status, _From, #state{qpid = QPid,
                                  http_pid = HttpPid}) ->
    {reply, [{qpid, QPid},
             {http_pid, HttpPid}]};
handle_call(BadMsg, _From, State) ->
    {reply, {error, {unknown, BadMsg}}, State}.


handle_cast({ready, Index, QPid}, State) ->
    yz_solrq:request_batch(QPid, Index, self()),
    {noreply, State};
handle_cast({batch, Index, Entries0}, State) ->
    try
        %% TODO: use ibrowse http worker
        %% TODO: batch updates to YZ AAE
        %% Unique the entries and lookup things needed for SOLR/AAE
        Entries = [{BKey, Obj, Reason, P, riak_kv_util:get_index_n(BKey), yz_kv:hash_object(Obj)} ||
                      {BKey, Obj, Reason, P} <- lists:ukeysort(1, Entries0)],
        case update_solr(Index, Entries) of
            ok ->
                update_aae(Entries);
            {error, Reason} ->
                %% SOLR Error, do not update AAE trees
                ok
        end,
        {noreply, State}
    catch
        _:Err ->
            Trace = erlang:get_stacktrace(),
            lager:info("index failed - ~p\nat: ~p", [Err, Trace]),
            {noreply, State}
    end.

%% Entries is [{Index, BKey, Obj, Reason, P, ShortPL, Hash}]
update_solr(Index, Entries) ->
    case yz_kv:should_index(Index) of
        false ->
            ok; % No need to send anything to SOLR, still need for AAE.
        _ ->
            send_solr_ops(Index, solr_ops(Entries))
    end.

%% Build the SOLR query
solr_ops(Entries) ->
    Ring = yz_misc:get_ring(transformed),
    LI = yz_cover:logical_index(Ring),
    lists:reverse(
      lists:foldl(
        fun({BKey, Obj, Reason, P, ShortPL, Hash}, Ops) ->
                %% TODO: This should be called in yz_solr:index
                %% then the ring lookup can be removed
                case yz_kv:is_owner_or_future_owner(P, node(), Ring) of
                    true ->
                        case Reason of
                            delete ->
                                [{delete, yz_solr:encode_delete({bkey, BKey})} | Ops];
                            _ ->
                                LFPN = yz_cover:logical_partition(LI, element(1, ShortPL)),
                                LP = yz_cover:logical_partition(LI, P),
                                AddOps = yz_doc:make_docs(Obj, Hash, ?INT_TO_BIN(LFPN), ?INT_TO_BIN(LP)),
                                DeleteOps = yz_kv:delete_operation(Obj, Reason, AddOps, BKey, LP),
                                %% List will be reversed, so make sure deletes happen before adds
                                lists:append([[{add, yz_solr:encode_doc(Doc)} || Doc <- AddOps],
                                              [{delete, yz_solr:encode_delete(DeleteOp)} || DeleteOp <- DeleteOps],
                                              Ops])
                        end;
                    false ->
                        Ops
                end
        end, [], Entries)).


send_solr_ops(Index, Ops) ->
    try
        T1 = os:timestamp(),
        case yz_solr:index_batch(Index, Ops) of
            ok ->
                yz_stat:index_end(Index, length(Ops), ?YZ_TIME_ELAPSED(T1)),
                ok;
            ER ->
                yz_stat:index_fail(),
                ER
        end
    catch _:Err ->
            yz_stat:index_fail(),
            Trace = erlang:get_stacktrace(),
            {error, {Err, Trace}}
    end.

update_aae(Entries) ->
    lists:foreach(fun({BKey, _Obj, Reason0, P, ShortPL, Hash}) ->
                          Action = case Reason0 of
                                       delete -> delete;
                                       _ -> {insert, Hash}
                                   end,
                          yz_kv:update_hashtree(Action, P, ShortPL, BKey)
                  end, Entries).

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
