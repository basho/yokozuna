%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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
%%-------------------------------------------------------------------
-module(yz_test_listener).

-behaviour(gen_server).

%% API
-export([start/0, stop/0, messages/0, clear/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {messages=[]}).

%%%===================================================================
%%% API
%%%===================================================================

start() ->
    {ok, _Pid} = gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call({global, ?SERVER}, stop).

messages() ->
    gen_server:call({global, ?SERVER}, messages).

clear() ->
    gen_server:call({global, ?SERVER}, clear).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({message, Message}, _From, #state{messages = Messages} = State) ->
    {reply, ok, State#state{messages=[Message | Messages]}};
handle_call(messages, _From, #state{messages = Messages} = State) ->
    {reply, Messages, State};
handle_call(clear, _From, State) ->
    {reply, ok, State#state{messages=[]}};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

%handle_info({message, Message}, #state{messages = Messages} = State) ->
%    {noreply, State#state{messages=[Message | Messages]}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
