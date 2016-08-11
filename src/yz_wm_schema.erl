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


%% @doc Resource for managing Yokozuna/Solr Schemas over HTTP.
%%
%% Available operations:
%%
%% `GET /search/schema/Schema'
%%
%%   Retrieves the schema with the given name
%%
%% `PUT /search/schema/Schema'
%%
%%   Uploads a schema with the given name
%%
%%   A PUT request requires this header:
%%
%%     `Content-Type: application/xml'
%%
%%   A Solr schema is expected as body
%%
%%

-module(yz_wm_schema).
-compile(export_all).
-include("yokozuna.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(ctx, {schema_name :: string(), %% name the schema
              method :: atom(),        %% HTTP method for the request
              security                 %% security context
              }).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return the list of routes provided by this resource.
routes() ->
    [{["search", "schema", schema], yz_wm_schema, []}].


%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_Props) ->
    {ok, #ctx{}}.

service_available(RD, Ctx=#ctx{}) ->
    {true,
        RD,
        Ctx#ctx{
            method=wrq:method(RD),
            schema_name=mochiweb_util:unquote(wrq:path_info(schema, RD))}
    }.

allowed_methods(RD, S) ->
    Methods = ['PUT', 'GET'],
    {Methods, RD, S}.

content_types_provided(RD, S) ->
    Types = [{"application/xml", read_schema}],
    {Types, RD, S}.

content_types_accepted(RD, S) ->
    Types = [{"application/xml", store_schema}],
    {Types, RD, S}.

resource_exists(RD, S) ->
    SchemaName = S#ctx.schema_name,
    {yz_schema:exists(list_to_binary(SchemaName)), RD, S}.

malformed_request(RD, S) ->
    case S#ctx.schema_name of
        undefined -> {{halt, 404}, RD, S};
        _ -> {false, RD, S}
    end.

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

%% Uses the riak_kv,secure_referer_check setting rather
%% as opposed to a special yokozuna-specific config
forbidden(RD, Ctx=#ctx{security=undefined}) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx};
forbidden(RD, Ctx=#ctx{security=Security}) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, Ctx};
        false ->
            PermAndResource = {?YZ_SECURITY_ADMIN_PERM, ?YZ_SECURITY_SCHEMA},
            Res = riak_core_security:check_permission(PermAndResource, Security),
            case Res of
                {false, Error, _} ->
                    {true, wrq:append_to_resp_body(Error, RD), Ctx};
                {true, _} ->
                    {false, RD, Ctx}
            end
    end.
%% Responds to a PUT request by storing the schema
%% Will overwrite schema with the same name
store_schema(RD, S) ->
    SchemaName = S#ctx.schema_name,
    Schema = wrq:req_body(RD),
    case yz_schema:store(list_to_binary(SchemaName), Schema) of
        ok  ->
            {true, RD, S};
        {error, Reason} ->
            Msg = io_lib:format("Error storing schema: ~s~n", [Reason]),
            RD2 = wrq:append_to_response_body(Msg, RD),
            RD3 = wrq:set_resp_header("Content-Type", "text/plain", RD2),
            {{halt,400}, RD3, S}
    end.

%% Responds to a GET request by returning schema
read_schema(RD, S) ->
    SchemaName = S#ctx.schema_name,
    {ok, RawSchema} = yz_schema:get(list_to_binary(SchemaName)),
    {RawSchema, RD, S}.
