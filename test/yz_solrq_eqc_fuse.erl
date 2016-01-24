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
-module(yz_solrq_eqc_fuse).

-behaviour(gen_server).

%% API
-export([start_link/0, stop/0, reset/0, ask/2, melt/1, melts/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
-compile([export_all, {parse_transform, pulse_instrument}]).
-compile({pulse_replace_module, [{gen_server, pulse_gen_server}]}).
-endif.

-define(SERVER, ?MODULE).

-record(state, {
    indices = dict:new(),
    threshold = 1, % 1 melt is enough to blow a fuse
    interval = 100  % recover after 100ms
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

reset() ->
    gen_server:call(?MODULE, reset).

ask(Index, Context) ->
    gen_server:call(?MODULE, {ask, Index, Context}).

melt(Index) ->
    gen_server:cast(?MODULE, {melt, Index}).

melts(Index) ->
    %% NB. yz_fuse converts Indices (as binaries) to atoms, so in general,
    %% melt and ask are called with atom arguments.  Since this function
    %% is used just for property testing (and is not called through yz_fuse),
    %% we need to covert the Index paramter from a binary to an atom.
    gen_server:call(?MODULE, {melts, list_to_atom(binary_to_list(Index))}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call(reset, _From, _State) ->
    {reply, ok, #state{}};

handle_call(
    {ask, Index, _Context}, _From, #state{indices = Indices} = State
) ->
    IndexState =
        case dict:find(Index, Indices) of
            {ok, {blown, _Melts}} ->
                blown;
            _ ->
                ok
        end,
    {reply, IndexState, State};

handle_call({melts, Index}, _From, #state{indices = Indices} = State) ->
    TotalMelts = case dict:find(Index, Indices) of
        {ok, {_FuseState, Melts}} -> Melts;
        _ -> 0
    end,
    {reply, TotalMelts, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({melt, Index}, #state{indices = Indices, threshold = Threshold, interval = Interval} = State) ->
    NewMelts =
        case dict:find(Index, Indices) of
            {ok, {_FuseState, Melts}} ->
                Melts + 1;
            error ->
                1
        end,
    FuseState =
        case NewMelts == Threshold of
            true ->
                yz_solrq_sup:blown_fuse(to_binary(Index)),
                erlang:send_after(Interval, ?MODULE, {recover, Index}),
                blown;
            _ ->
                ok
        end,
    {noreply, State#state{indices = dict:store(Index, {FuseState, NewMelts}, Indices)}};

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({recover, Index}, #state{indices=Indices} = State) ->
    {ok, {_FuseState, Melts}} = dict:find(Index, Indices),
    yz_solrq_sup:healed_fuse(to_binary(Index)),
    {noreply, State#state{indices=dict:store(Index, {ok, Melts}, Indices)}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

to_binary(Atom) ->
    list_to_binary(atom_to_list(Atom)).