%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(yz_wm_search).
-compile(export_all).
-include("yokozuna.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return the list of routes provided by this resource.
routes() ->
    [{["search", index], yz_wm_search, []}].


%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_) ->
    {ok, none}.

allowed_methods(Req, S) ->
    Methods = ['GET', 'POST'],
    {Methods, Req, S}.

content_types_provided(Req, S) ->
    Types = [{"text/xml", search}],
    {Types, Req, S}.

%% Treat POST as GET in order to work with existing Solr clients.
process_post(Req, S) ->
    case search(Req, S) of
        {Val, Req2, S2} when is_binary(Val) ->
            Req3 = wrq:set_resp_body(Val, Req2),
            {true, Req3, S2};
        Other ->
            %% In this case assume Val is `{halt,Code}' or
            %% `{error,Term}'
            Other
    end.

search(Req, S) ->
    Index = wrq:path_info(index, Req),
    Params = wrq:req_qs(Req),
    Mapping = yz_events:get_mapping(),
    ReqHeaders = mochiweb_headers:to_list(wrq:req_headers(Req)),
    try
        {RespHeaders, Body} = yz_solr:search(Index, ReqHeaders,
                                             Params, Mapping),
        Req2 = wrq:set_resp_headers(scrub_headers(RespHeaders), Req),
        {Body, Req2, S}
    catch throw:insufficient_vnodes_available ->
            ErrReq = wrq:set_resp_header("Content-Type", "text/plain", Req),
            ErrReq2 = wrq:set_resp_body(?YZ_ERR_NOT_ENOUGH_NODES ++ "\n",
                                        ErrReq),
            {{halt, 503}, ErrReq2, S}
    end.

scrub_headers(RespHeaders) ->
    %% Solr returns as chunked but not going to return as chunked from
    %% Yokozuna.
    lists:keydelete("Transfer-Encoding", 1, RespHeaders).
