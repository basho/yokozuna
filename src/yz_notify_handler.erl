%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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
%%
%% @doc This module contains functionality related to integrating with
%%      Riak KV.

-module(yz_notify_handler).
-behaviour(gen_server).

-export([start_link/0, stop/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          url,
          sock
         }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

init([]) ->
    process_flag(trap_exit, true),
    Url = app_helper:get_env(riak_kv, notifier_url, "inproc://riak_kv"),
    self() ! connect,
    {ok, #state{url=Url}}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(connect, #state{url=Url, sock=undefined}=State) ->
    case enm:sub([{connect, Url},{subscribe, <<"riak_kv_vnode">>}]) of
        {ok, Sock} ->
            ok = enm:setopts(Sock, [{active,once}]),
            {noreply, State#state{sock=Sock}};
        _Error ->
            erlang:send_after(1000, self(), connect),
            {noreply, State}
    end;
handle_info({nnsub, Sock, <<"riak_kv_vnode", Bin/binary>>},
            #state{sock=Sock}=State) ->
    case binary_to_term(Bin) of
        {index, Obj, Reason, Partition} ->
            yz_kv:index(Obj, Reason, Partition);
        {should_handoff, X} ->
            yz_kv:should_handoff(X)
    end,
    ok = enm:setopts(Sock, [{active,once}]),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{sock=undefined}) ->
    ok;
terminate(_Reason, #state{sock=Sock}) ->
    enm:close(Sock),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
