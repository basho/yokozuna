%% -------------------------------------------------------------------
%%
%% yz_eventhandler_sup: Supervise processes for persistent event handlers.
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

%% @doc Supervise yz_eventhandler_guard processes.
%%      Akin to (and wraps some of) riak_core_eventhandler_sup.erl and uses
%%      TODO: Create generic eventhandler_sup and eventhandler_guard templates.

-module(yz_eventhandler_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([start_guarded_handler/3, start_guarded_handler/4, stop_guarded_handler/3]).

start_guarded_handler(HandlerMod, Handler, Args) ->
    start_guarded_handler(HandlerMod, Handler, Args, undefined).

start_guarded_handler(HandlerMod, Handler, Args, ExitFun) ->
    case supervisor:start_child(?MODULE, handler_spec(HandlerMod, Handler, Args, ExitFun)) of
        {ok, _Pid} -> ok;
        Other -> Other
    end.

stop_guarded_handler(HandlerMod, Handler, Args) ->
    %% reuse already generic riak_core_eventhandler_sup:stop_guarded_handler
    riak_core_eventhandler_sup:stop_guarded_handler(HandlerMod, Handler, Args).

handler_spec(HandlerMod, Handler, Args, ExitFun) ->
    {{HandlerMod, Handler},
     {yz_eventhandler_guard, start_link, [HandlerMod, Handler, Args, ExitFun]},
     transient, 5000, worker, [yz_eventhandler_guard]}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 10, 10}, []}}.
