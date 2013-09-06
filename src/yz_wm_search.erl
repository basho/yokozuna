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
-define(YZ_HEAD_FPROF, "yz-fprof").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return the list of routes provided by this resource.
-spec routes() -> [tuple()].
routes() ->
    Routes1 = [{["search", index], ?MODULE, []}],
    case yz_misc:is_riak_search_enabled() of
        false ->
            [{["solr", index, "select"], ?MODULE, []}|Routes1];
        true ->
            Routes1
    end.


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

service_available(Req, S) ->
    {yokozuna:is_enabled(search), Req, S}.

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
    {FProf, FProfFile} = check_for_fprof(Req),
    ?IF(FProf, fprof:trace(start, FProfFile)),
    Index = wrq:path_info(index, Req),
    Params = wrq:req_qs(Req),
    ReqHeaders = mochiweb_headers:to_list(wrq:req_headers(Req)),
    try
        Result = yz_solr:dist_search(Index, ReqHeaders, Params),
        case Result of
            {error, insufficient_vnodes_available} ->
                ER1 = wrq:set_resp_header("Content-Type", "text/plain", Req),
                ER2 = wrq:set_resp_body(?YZ_ERR_NOT_ENOUGH_NODES ++ "\n", ER1),
                {{halt, 503}, ER2, S};
            {RespHeaders, Body} ->
                Req2 = wrq:set_resp_headers(scrub_headers(RespHeaders), Req),
                {Body, Req2, S}
        end
    catch
        throw:not_found ->
            ErrReq = wrq:append_to_response_body(
                io_lib:format(?YZ_ERR_INDEX_NOT_FOUND ++ "\n", [Index]),
                Req),
            ErrReq2 = wrq:set_resp_header("Content-Type", "text/plain",
                                        ErrReq),
            {{halt, 404}, ErrReq2, S};
        throw:{solr_error, {Code, _URL, Err}} ->
            ErrReq = wrq:append_to_response_body(Err, Req),
            ErrReq2 = wrq:set_resp_header("Content-Type", "text/plain",
                                        ErrReq),
            {{halt, Code}, ErrReq2, S}
    after
        ?IF(FProf, fprof_analyse(FProfFile))
    end.

scrub_headers(RespHeaders) ->
    %% Solr returns as chunked but not going to return as chunked from
    %% Yokozuna.
    lists:keydelete("Transfer-Encoding", 1, RespHeaders).

check_for_fprof(Req) ->
    case wrq:get_req_header(?YZ_HEAD_FPROF, Req) of
        undefined -> {false, none};
        File -> {true, File}
    end.

fprof_analyse(FileName) ->
    fprof:trace(stop),
    fprof:profile(file, FileName),
    fprof:analyse([{dest, FileName ++ ".analysis"}, {cols, 120}]).
