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
-export([start_link/0, get_lock/0, release_lock/0, drain/0, drain/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

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


get_lock() ->
    gen_server:call(?SERVER, {get_lock, self()}, infinity).

release_lock() ->
    gen_server:call(?SERVER, {release_lock, self()}, infinity).

%% @doc Drain all queues to Solr
-spec drain() -> ok | {error, _Reason}.
drain() ->
    drain(fun() -> ok end).

%% @doc Drain all queues to Solr
-spec drain(fun(() -> ok)) -> ok | {error, _Reason}.
drain(DrainCompleteCallback) ->
    case get_lock() of
        ok ->
            DrainTimeout = application:get_env(yokozuna, drain_timeout, 1000),
            try
                {ok, Pid} = yz_solrq_sup:start_drain_fsm(DrainCompleteCallback),
                Reference = erlang:monitor(process, Pid),
                yz_solrq_drain_fsm:start_prepare(),
                receive
                    {'DOWN', Reference, _Type, _Object, normal} ->
                        ok;
                    {'DOWN', Reference, _Type, _Object, Info} ->
                        {error, Info}
                after DrainTimeout ->
                    erlang:demonitor(Reference),
                    %yz_solrq_drain_fsm:maybe_cancel()
                    {error, timeout}
                end
            after
                release_lock()
            end;
        _ ->
            {error, in_progress}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
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

handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

maybe_release_lock(#state{lock=undefined} = S, _) ->
    S;
maybe_release_lock(#state{lock={Ref, Pid}} = S, Pid) ->
    demonitor(Ref),
    S#state{lock=undefined};
maybe_release_lock(#state{lock={Ref, _Pid}} = S, Ref) ->
    S#state{lock=undefined};
maybe_release_lock(#state{lock={_Ref, _Pid}} = S, _) ->
    S.