%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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
-module(yz_solrq_drain_mgr).

-behaviour(gen_server).

%% API
-export([start_link/0, drain/0, drain/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-include("yokozuna.hrl").

-define(SERVER, ?MODULE).

-record(state, {
    lock = undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Drain all queues to Solr
-spec drain() -> ok | {error, _Reason}.
drain() ->
    drain([]).

%% @doc Drain all queues to Solr
-spec drain(drain_params()) -> ok | {error, _Reason}.
drain(Params) ->
    T1 = os:timestamp(),
    ExchangeFSMPid = proplists:get_value(
        ?EXCHANGE_FSM_PID, Params, undefined
    ),
    case enabled() of
        true ->
            case get_lock() of
                ok ->
                    DrainTimeout = application:get_env(?YZ_APP_NAME, drain_timeout, 60000),
                    try
                        {ok, Pid} = yz_solrq_sup:start_drain_fsm(Params),
                        Reference = erlang:monitor(process, Pid),
                        yz_solrq_drain_fsm:start_prepare(),
                        receive
                            {'DOWN', Reference, process, Pid, normal} ->
                                yz_stat:drain_end(?YZ_TIME_ELAPSED(T1)),
                                ok;
                            {'DOWN', Reference, process, Pid, Reason} ->
                                yz_stat:drain_fail(),
                                maybe_exchange_fsm_drain_error(ExchangeFSMPid, Reason),
                                {error, Reason}
                        after DrainTimeout ->
                            yz_stat:drain_timeout(),
                            cancel(Reference, Pid),
                            maybe_exchange_fsm_drain_error(ExchangeFSMPid, timeout),
                            {error, timeout}
                        end
                    after
                        release_lock()
                    end;
                drain_already_locked ->
                    maybe_exchange_fsm_drain_error(ExchangeFSMPid, in_progress),
                    {error, in_progress}
            end;
        _ ->
            YZIndexHashtreeUpdateParams = proplists:get_value(
                ?YZ_INDEX_HASHTREE_PARAMS, Params
            ),
            maybe_update_yz_index_hashtree(
                ExchangeFSMPid, YZIndexHashtreeUpdateParams
            ),
            yz_stat:drain_end(?YZ_TIME_ELAPSED(T1)),
            ok
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    schedule_tick(),
    {ok, #state{}}.


handle_call({get_lock, Pid}, _From, #state{lock=undefined} = State) ->
    Ref = monitor(process, Pid),
    State2 = State#state{lock={Ref, Pid}},
    {reply, ok, State2};

handle_call({get_lock, Pid}, _From, #state{lock={_Ref, Pid}} = State) ->
    {reply, ok, State};

handle_call({get_lock, _Pid}, _From, State) ->
    {reply, drain_already_locked, State};

handle_call({release_lock, Pid}, _From, State) ->
    {reply, ok, maybe_release_lock(State, Pid)};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast(_Request, State) ->
    {noreply, State}.


handle_info({'DOWN', Ref, _, _Obj, _Status}, State) ->
    {noreply, maybe_release_lock(State, Ref)};

handle_info(tick, State) ->
    yz_stat:queue_capacity(yz_solrq_sup:queue_capacity()),
    schedule_tick(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_lock() ->
    gen_server:call(?SERVER, {get_lock, self()}, infinity).

release_lock() ->
    gen_server:call(?SERVER, {release_lock, self()}, infinity).

enabled() ->
    application:get_env(?YZ_APP_NAME, enable_drains, true).

maybe_release_lock(#state{lock=undefined} = S, _) ->
    S;
maybe_release_lock(#state{lock={Ref, Pid}} = S, Pid) ->
    demonitor(Ref),
    S#state{lock=undefined};
maybe_release_lock(#state{lock={Ref, _Pid}} = S, Ref) ->
    S#state{lock=undefined};
maybe_release_lock(#state{lock={_Ref, _Pid}} = S, _) ->
    S.

cancel(Reference, Pid) ->
    case yz_solrq_drain_fsm:cancel() of
        ok ->
            receive
                {'DOWN', Reference, process, Pid, normal} ->
                    ok
            after 5000 ->
                unlink_and_kill(Reference, Pid),
                {error, timeout}
            end;
        no_proc ->
            ok;
        timeout ->
            unlink_and_kill(Reference, Pid),
            {error, timeout}
    end.

unlink_and_kill(Reference, Pid) ->
    try
        demonitor(Reference),
        unlink(Pid),
        exit(Pid, kill)
    catch _:_ ->
        ok
    end.

-spec schedule_tick() -> reference().
schedule_tick() ->
    erlang:send_after(5000, ?MODULE, tick).


maybe_exchange_fsm_drain_error(undefined, _Reason) ->
    ok;
maybe_exchange_fsm_drain_error(Pid, Reason) ->
    yz_exchange_fsm:drain_error(Pid, Reason).

maybe_update_yz_index_hashtree(undefined, undefined) ->
    ok;
maybe_update_yz_index_hashtree(Pid, {YZTree, Index, IndexN}) ->
    yz_exchange_fsm:update_yz_index_hashtree(Pid, YZTree, Index, IndexN).
