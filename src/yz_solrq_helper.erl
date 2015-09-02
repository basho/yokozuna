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
-export([start_link/1, status/1, status/2, queue_entries/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {qpid,
                http_pid}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(QPid) ->
    gen_server:start_link(?MODULE, [QPid], []).

status(Pid) ->
    status(Pid, 60000). % solr can block, long timeout by default

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

queue_entries(HPid, Entries) ->
    gen_server:cast(HPid, {entries, Entries}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([QPid]) ->
    Host = "localhost",
    Port = yz_solr:port(),
    %% TODO: Prolly need to monitor
    {ok, HttpPid} = ibrowse_http_client:start({Host, Port}),
    yz_solrq:poll(QPid, self()),
    {ok, #state{qpid = QPid, http_pid = HttpPid}}.

handle_call(status, _From, #state{qpid = QPid,
                                  http_pid = HttpPid}) ->
    {reply, [{qpid, QPid},
             {http_pid, HttpPid}]};
handle_call(BadMsg, _From, State) ->
    {reply, {error, {unknown, BadMsg}}, State}.

handle_cast({entries, Entries0}, #state{qpid = QPid,
                                       http_pid = _HttpPid} = State) ->
    try
        %% TODO: use ibrowse http worker
        %% TODO: batch updates to YZ AAE
        Ring = yz_misc:get_ring(transformed),
        T1 = os:timestamp(),
        Entries = [{Index, BKey, Obj, Reason, P, riak_kv_util:get_index_n(BKey), yz_kv:hash_object(Obj)} ||
            {Index, BKey, Obj, Reason, P} <- Entries0],
        SolrUpdates = update_solr(Entries, Ring),
        update_aae(SolrUpdates, Entries),
        %% TODO: Since this is a batch now, do we need to do something different with the stat?
        yz_stat:index_end(?YZ_TIME_ELAPSED(T1)),
        yz_solrq:poll(QPid, self()),
        {noreply, State}
    catch
        _:Err ->
            Trace = erlang:get_stacktrace(),
            lager:info("index failed - ~p\nat: ~p", [Err, Trace]),
            {noreply, State}
    end.

%% Entries is [{Index, BKey, Obj, Reason, P, ShortPL, Hash}]
update_solr(Entries, Ring) ->
    LI = yz_cover:logical_index(Ring),
    OpsByIndex =
        lists:foldl(fun({Index, BKey, Obj, Reason, P, ShortPL, Hash}, OpsByIndex0) ->
            case yz_kv:is_owner_or_future_owner(P, node(), Ring) of
                true ->
                    EncodedOps = case Reason of
                        delete ->
                            [{delete, yz_solr:encode_delete([{bkey, BKey}])}];
                        _ ->
                            LFPN = yz_cover:logical_partition(LI, element(1, ShortPL)),
                            LP = yz_cover:logical_partition(LI, P),
                            AddOps = yz_doc:make_docs(Obj, Hash, ?INT_TO_BIN(LFPN), ?INT_TO_BIN(LP)),
                            DeleteOps = yz_kv:delete_operation(Obj, Reason, AddOps, BKey, LP),
                            [{delete, yz_solr:encode_delete(DeleteOp)} || DeleteOp <- DeleteOps] ++
                            [{add, yz_solr:encode_doc(Doc)} || Doc <- AddOps]
                    end,
                    dict:merge(
                        %% TODO: Could remove duplicat add ops here?
                        fun(_Key, OldOps, NewOps) -> OldOps ++ NewOps end,
                        OpsByIndex0,
                        dict:from_list([{Index, EncodedOps}]));
                _ -> OpsByIndex0
            end
            end,
            dict:new(),
            Entries),
    send_solr_ops(dict:to_list(OpsByIndex)).

send_solr_ops(OpsByIndex) ->
    lists:map(fun({Index, Ops}) ->
        try
            yz_solr:index_batch(Index, Ops),
            {Index, ok}
        catch _:Err ->
            yz_stat:index_fail(),
            Trace = erlang:get_stacktrace(),
            {Index, Err, Trace}
        end
    end, OpsByIndex).


update_aae(SolrResults, Entries) ->
    lists:foreach(fun({Index, BKey, _Obj, Reason0, P, ShortPL, Hash}) ->
        Reason = case Reason0 of
            delete -> delete;
            _ -> insert
        end,
        case proplists:get_value(Index, SolrResults) of
            ok ->
                yz_kv:update_hashtree({Reason, Hash}, P, ShortPL, BKey);
            {_Err, _Trace} ->
                %% TODO: What do we do with a failed index batch? Index docs individually again?
                ok
        end
        end, Entries).

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
