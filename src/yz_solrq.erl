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
-module(yz_solrq).

%% api
-export([start_link/1, index/5, poll/2, set_hwm/2, set_batch/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {helper_pid,
                queue = queue:new(),
                queue_len = 0, % otherwise Q module counts lists
                queue_hwm = 10000,
                batch_min = 100,
                batch_max = 1000,
                pending_vnodes = [],
                pending_helpers = []}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

index(Index, BKey, Obj, Reason, P) ->
    %% Hash on the index and bkey to make sure all updates to an
    %% individual object in an index are serialized
    Hash = erlang:phash2({Index, BKey}),
    gen_server:call(yz_solrq_sup:regname(Hash),
                    {index, {Index, BKey, Obj, Reason, P}}, infinity).

poll(QPid, HPid) ->
    gen_server:cast(QPid, {poll, HPid}).

set_hwm(QPid, HWM) ->
    gen_server:call(QPid, {set_hwm, HWM}).

set_batch(QPid, Min, Max) ->
    gen_server:call(QPid, {set_batch, Min, Max}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
    schedule_tick(),
    {ok, Helper} = yz_solrq_helper:start_link(self()),
    {ok, #state{helper_pid = Helper}} .

handle_call({index, E}, From,
            #state{pending_vnodes = PendingVnodes} = State) ->
    State2 = enqueue(E, State),
    State3 = maybe_send_entries(State2),
    case over_hwm(State3) of % nicer to call maybe_reply, but saving list cons
        true ->
            yz_stat:blocked_vnode(From),
            {noreply, State3#state{pending_vnodes = [From | PendingVnodes]}};
        _ ->
            {reply, ok, State3}
    end;
handle_call({set_hwm, NewHWM}, _From, #state{queue_hwm = OldHWM} = State) ->
    {reply, {ok, OldHWM}, State#state{queue_hwm = NewHWM}};
handle_call({set_batch, Min, Max}, _From,
            #state{batch_min = OldMin, batch_max = OldMax} = State) ->
    State2 = maybe_send_entries(State#state{batch_min = Min, batch_max = Max}),
    {reply, {ok, {OldMin, OldMax}}, State2}.


handle_cast({poll, HPid}, #state{queue_len = L, batch_min = Min} = State) ->
    case L >= Min of
        true ->
            {noreply, send_entries(HPid, State)};
        _ ->
            {noreply, State#state{pending_helpers = [HPid]}}
    end.

handle_info(tick, State) ->
    {noreply, tick(State)};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private
%%%===================================================================

%% Enqueue the entry and return updated state.
enqueue(E, #state{queue = Q, queue_len = L} = State) ->
    State#state{queue = queue:in(E, Q), queue_len = L + 1}.

%% Return true if queue is over the high water mark
over_hwm(#state{queue_len = L, queue_hwm = HWM}) ->
    L > HWM.

%% Send entries if the helper is available AND queue meets
%% the minimum batch size, then return state
maybe_send_entries(#state{pending_helpers = [HPid],
                          queue_len = L,
                          batch_min = Min} = State) when L >= Min ->
    send_entries(HPid, State#state{pending_helpers = []});
maybe_send_entries(State) ->
    State.

%% Send a batch of entries, reply to any blocked vnodes and
%% return updated state
send_entries(HPid, #state{queue = Q,
                          queue_len = L,
                          batch_max = Max} = State) ->
    SendSize = min(L, Max),
    {BatchQ, RestQ} = queue:split(SendSize, Q),
    yz_solrq_helper:queue_entries(HPid, queue:to_list(BatchQ)),
    maybe_reply(State#state{queue = RestQ,
                            queue_len = L - SendSize}).

%% Send replies to blocked vnodes if under the high water mark
%% and return updated state
maybe_reply(#state{pending_vnodes = []} = State) ->
    State;
maybe_reply(#state{pending_vnodes = PendingVnodes} = State) ->
    case over_hwm(State) of
        true ->
            State;
        _ ->
            _ = [gen_server:reply(From, ok) || From <- PendingVnodes],
            State#state{pending_vnodes = []}
    end.

%% Periodic tick, make sure we eventually deliver to SOLR
%% even if the load stops.
tick(#state{pending_helpers = Helpers} = State) ->
    schedule_tick(),
    case Helpers of
        [] ->
            State;
        [HPid] ->
            send_entries(HPid, State#state{pending_helpers = []})
    end.

schedule_tick() ->
    timer:send_after(tick_delay(), self(), tick).

tick_delay() ->
    application:get_env(yokozuna, solrq_tick, 100).
