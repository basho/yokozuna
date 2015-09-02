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

handle_cast({entries, Entries}, #state{qpid = QPid,
                                       http_pid = _HttpPid} = State) ->
    try
        %% TODO: use ibrowse http worker
        %% TODO: batch updates to SOLR
        %% TODO: batch updates to YZ AAE
        _ = [index(Index, BKey, Obj, Reason, P) ||
                {Index, BKey, Obj, Reason, P} <- Entries],
        yz_solrq:poll(QPid, self()),
        {noreply, State}
    catch
        _:Err ->
            lager:info("index failed - ~p\n", [Err]),
            {noreply, State}
    end.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

index(Index, BKey, Obj, Reason, P) ->
    Ring = yz_misc:get_ring(transformed),
    case yz_kv:is_owner_or_future_owner(P, node(), Ring) of
        true ->
            T1 = os:timestamp(),
            try
                ShortPL = riak_kv_util:get_index_n(BKey),
                case yz_kv:should_index(Index) of
                    true ->
                        yz_kv:index(Obj, Reason, Ring, P, BKey, ShortPL, Index);
                    false ->
                        yz_kv:dont_index(Obj, Reason, P, BKey, ShortPL)
                end,
                yz_stat:index_end(?YZ_TIME_ELAPSED(T1))
            catch _:Err ->
                    yz_stat:index_fail(),
                    Trace = erlang:get_stacktrace(),
                    case Reason of
                        delete ->
                            ?ERROR("failed to delete docid ~p with error ~p because ~p",
                                   [BKey, Err, Trace]);
                        _ ->
                            ?ERROR("failed to index object ~p with error ~p because ~p",
                                   [BKey, Err, Trace])
                    end
            end;
        false ->
            ok
    end.
