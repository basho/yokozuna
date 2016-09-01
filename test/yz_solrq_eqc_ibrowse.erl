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
-module(yz_solrq_eqc_ibrowse).

-behaviour(gen_server).

%% API
-export([start_link/1, stop/0, get_response/1, keys/0, wait/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-ifdef(EQC).
%% -define(EQC_DEBUG(S, F), eqc:format(S, F)).
-define(EQC_DEBUG(S, F), ok).
-else.
-define(EQC_DEBUG(S, F), ok).
-endif.

-define(SERVER, ?MODULE).

-record(state, {
    keyres,
    written = [],
    failed = [],
    root = undefined, expected = []
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(KeyRes) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, KeyRes, []).

stop() ->
    gen_server:stop(?MODULE).

get_response(B) ->
    gen_server:call(?MODULE, {get_response, B}).

keys() ->
    gen_server:call(?MODULE, keys).

wait(Keys) ->
    gen_server:call(?MODULE, {wait, Keys}, 1000).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(KeyRes) ->
    {ok, #state{keyres = KeyRes}}.

handle_call(
    {get_response, B}, _From,
    #state{
        keyres=KeyRes,
        written=Written,
        failed=AlredyFailed,
        root=Root,
        expected=Expected
    } = State
) ->
    ?EQC_DEBUG("In get_response", []),
    SolrReqs = parse_solr_reqs(mochijson2:decode(B)),
    {Keys, Res, NewFailed} = get_response(SolrReqs, KeyRes, AlredyFailed),
    WrittenKeys = case Res of
        {ok, "200", _Some, _Crap} ->
            [Key || {_Op, Key} <- SolrReqs];
        _ ->
            []
    end,
    NewWritten = Written ++ WrittenKeys,
    case lists:usort(NewWritten) == Expected of
        true ->
            maybe_reply(Root);
        _ ->
            proceed
    end,
    ?EQC_DEBUG("yz_solrq_eqc_ibrowse: response: ~p~n", [{Keys, Res}]),
    {reply, {Keys, Res}, State#state{written = NewWritten, failed = NewFailed}};

handle_call(keys, _From, #state{written = Written} = State) ->
    {reply, Written, State};

handle_call({wait, Keys}, From, #state{written=Written} = State) ->
    case Keys of
        [] ->
            {reply, ok, State};
        _ ->
            case lists:usort(Written) == lists:usort(Keys) of
                true ->
                    {reply, ok, State};
                _ ->
                    ?EQC_DEBUG("Process ~p waiting for keys...: ~p", [From, Keys]),
                    {noreply, State#state{root=From, expected=lists:usort(Keys)}}
            end
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Returns [{add, Key}, {delete, Key}]
parse_solr_reqs({struct, Reqs}) ->
    [parse_solr_req(Req) || Req <- Reqs].

parse_solr_req({<<"add">>, {struct, [{<<"doc">>, Doc}]}}) ->
    {add, find_key_field(Doc)};
parse_solr_req({<<"delete">>, {struct, [{<<"query">>, Query}]}}) ->
    {delete, parse_delete_query(Query)};
parse_solr_req({delete, _Query}) ->
    {delete, could_parse_bkey};
parse_solr_req({Other, Thing}) ->
    {Other, Thing}.

find_key_field({struct, Props}) ->
    proplists:get_value(<<"yz_solrq_eqc">>, Props).

parse_delete_query(Query) ->
    {match, [Key]} = re:run(Query, "(XKEYX[0-9]+)",[{capture,[1],binary}]),
    Key.


%% Decide what to return for the request... If any of the seq
%% numbers had failures generated, apply to all of them, but
%% only if the request has not failed previously
get_response(SolrReqs, KeyRes, AlreadyFailed) ->
    Keys = lists:usort([Key || {_Op, Key} <- SolrReqs]),
    KeyResInSolrReqs = [{Key, Res} || {Key, Res} <- KeyRes, lists:member(Key, Keys)],
    true = length(KeyResInSolrReqs) > 0,
    {Response, NewFailed} = find_response(KeyResInSolrReqs, AlreadyFailed, undefined),
    {Keys, Response, NewFailed}.


find_response([], AlreadyFailed, Candidate) ->
    {Candidate, AlreadyFailed};
find_response([{Key, Res} | KeyResRest], AlreadyFailed, Candidate) ->
    case Candidate of
        {error, _Err} ->
            {Candidate, AlreadyFailed};
        {ok, "400", _Bad, _Request} ->
            {Candidate, AlreadyFailed};
        _ ->
            case Res of
                {error, _Err} ->
                    case lists:member(Key, AlreadyFailed) of
                        true ->
                            find_response(KeyResRest, AlreadyFailed, {ok, "200", some_other, crap});
                        _ ->
                            {Res, [Key | AlreadyFailed]}
                    end;
                _ ->
                    find_response(KeyResRest, AlreadyFailed, Res)
            end
    end.


maybe_reply(undefined) ->
    ok;
maybe_reply(Root) ->
    timer:sleep(500),
    gen_server:reply(Root, ok).