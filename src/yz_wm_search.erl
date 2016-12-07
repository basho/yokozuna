%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2016 Basho Technologies, Inc.
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

-record(ctx, {security      %% security context
             }).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return the list of routes provided by this resource.
-spec routes() -> [tuple()].
routes() ->
    Routes1 = [{["search", "query", index], ?MODULE, []}],
    case yz_rs_migration:is_riak_search_enabled() of
        false ->
            [{["solr", index, "select"], ?MODULE, []}|Routes1];
        true ->
            Routes1
    end.

%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_) ->
    {ok, #ctx{}}.

allowed_methods(Req, S) ->
    Methods = ['GET', 'POST'],
    {Methods, Req, S}.

content_types_provided(Req, S) ->
    Types = [{"text/xml", search}],
    {Types, Req, S}.

service_available(Req, S) ->
    {yokozuna:is_enabled(search), Req, S}.

is_authorized(ReqData, Ctx) ->
    case riak_api_web_security:is_authorized(ReqData) of
        false ->
            {"Basic realm=\"Riak\"", ReqData, Ctx};
        {true, SecContext} ->
            {true, ReqData, Ctx#ctx{security=SecContext}};
        insecure ->
            %% XXX 301 may be more appropriate here, but since the http and
            %% https port are different and configurable, it is hard to figure
            %% out the redirect URL to serve.
            {{halt, 426}, wrq:append_to_resp_body(<<"Security is enabled and "
                    "Riak does not accept credentials over HTTP. Try HTTPS "
                    "instead.">>, ReqData), Ctx}
    end.


resource_forbidden(RD, Ctx, _Perm, {_Resource, undefined}) ->
    {true, wrq:append_to_resp_body("Unknown index", RD), Ctx};
resource_forbidden(RD, Ctx=#ctx{security=Security}, Permission,
                   {Resource, Subresource}) ->
    Res = riak_core_security:check_permission(
            {Permission, {Resource, list_to_binary(mochiweb_util:unquote(Subresource))}},
            Security
           ),
    case Res of
        {false, Error, _} ->
            {true, wrq:append_to_resp_body(Error, RD), Ctx};
        {true, _} ->
            {false, RD, Ctx}
    end.

-spec forbidden(#wm_reqdata{}, #ctx{}) -> {boolean(), #wm_reqdata{}, #ctx{}}.
%% @doc Uses the riak_kv,secure_referer_check setting rather
%%      as opposed to a special yokozuna-specific config
forbidden(RD, Ctx=#ctx{security=undefined}) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx};
forbidden(RD, Ctx) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, Ctx};
        false ->
            resource_forbidden(RD, Ctx, ?YZ_SECURITY_SEARCH_PERM,
                               {?YZ_SECURITY_INDEX, wrq:path_info(index, RD)})
    end.

%% @doc Handle POST to work with existing Solr clients and long queries
%%      when the correct Content-Type is supplied.
process_post(RD, Ctx) ->
    CType = wrq:get_req_header(?HEAD_CTYPE, RD),
    case CType of
        "application/x-www-form-urlencoded" ->
            post_search(RD, Ctx, CType);
        _ ->
            {{halt, 415}, RD, Ctx}
    end.

search(Req, S) ->
    Params = wrq:req_qs(Req),
    search(Req, S, Params).

search(Req, S, Params) ->
    {FProf, FProfFile} = check_for_fprof(Req),
    ?IF(FProf, fprof:trace(start, FProfFile)),
    T1 = os:timestamp(),
    Index = list_to_binary(wrq:path_info(index, Req)),
    try
        Result = yz_solr:dist_search(Index, Params),
        case Result of
            {error, insufficient_vnodes_available} ->
                yz_stat:search_fail(),
                ER1 = wrq:set_resp_header("Content-Type", "text/plain", Req),
                ER2 = wrq:set_resp_body(?YZ_ERR_NOT_ENOUGH_NODES ++ "\n", ER1),
                {{halt, 503}, ER2, S};
            {error, OtherError} ->
                yz_stat:search_fail(),
                OtherErrorReq = wrq:append_to_response_body(
                                 io_lib:format("Error:~n~p~n", [OtherError]),
                                 Req),
                OtherErrorReq2 = wrq:set_resp_header("Content-Type",
                                                     "text/plain",
                                                     OtherErrorReq),

                {{halt, 500}, OtherErrorReq2, S};
            {RespHeaders, Body} ->
                yz_stat:search_end(?YZ_TIME_ELAPSED(T1)),
                Req2 = wrq:set_resp_headers(scrub_headers(RespHeaders), Req),
                {Body, Req2, S}
        end
    catch
        throw:{solr_error, {Code, _URL, Err}} ->
            yz_stat:search_fail(),
            ErrReq = wrq:append_to_response_body(Err, Req),
            ErrReq2 = wrq:set_resp_header("Content-Type", "text/plain",
                                        ErrReq),
            {{halt, Code}, ErrReq2, S}
    after
        ?IF(FProf, fprof_analyse(FProfFile))
    end.

%% @doc Solr returns as chunked but not going to return as chunked from
%%      Yokozuna.
scrub_headers(RespHeaders) when is_list(RespHeaders) ->
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

-spec resource_exists(term(), term()) -> {boolean(), term(), term()}.
resource_exists(RD, Context) ->
    IndexName = list_to_binary(wrq:path_info(index, RD)),
    IndexInfo = yz_index:get_index_info(IndexName),
    {undefined /= IndexInfo, RD, Context}.

%% ====================================================================
%% Private
%% ====================================================================

-spec decode_body_from_ctype(string(), binary()) -> term().
%% @doc Decode the req_body binary based on content type that can be used.
decode_body_from_ctype("application/x-www-form-urlencoded", V) ->
    mochiweb_util:parse_qs(V);
decode_body_from_ctype(_CT, V) ->
    V.

%% @doc Treat POST as GET after decoding body as params for Solr
%%      `select search
post_search(Req, S, CT) ->
    Body = wrq:req_body(Req),
    BodyParams = decode_body_from_ctype(CT, Body),
    case BodyParams of
        [] -> Params = wrq:req_qs(Req);
        _ -> Params = BodyParams
    end,
    case search(Req, S, Params) of
        {Val, Req2, S2} when is_binary(Val) ->
            Req3 = wrq:set_resp_body(Val, Req2),
            {true, Req3, S2};
        Other ->
            %% In this case assume Val is `{halt,Code}' or
            %% `{error,Term}'
            Other
    end.
