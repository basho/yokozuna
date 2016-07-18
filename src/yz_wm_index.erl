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


%% @doc Resource for managing Yokozuna Indexes over HTTP.
%%
%% Available operations:
%%
%% `GET /search/index'
%%
%%   Get information about every index in JSON format.
%%   Currently the same information as /search/index/Index,
%%   but as an array of JSON objects.
%%
%% `GET /search/index/Index'
%%
%%   Gets information about a specific index in JSON format.
%%   Returns the following information:
%% ```
%%   {
%%      "name"  : IndexName,
%%      "schema": SchemaName
%%   }
%% '''
%%   `IndexName' is the same value passed into the URL.
%%
%%   `Schema' is the name of the schema associate with this
%%   index. That schema file must already be installed on the server.
%%   Defaults to "_yz_default".
%%
%% `PUT /search/index/Index'
%%
%%   Creates a new index with the given name.
%%
%%   A PUT request requires this header:
%%
%%     `Content-Type: application/json'
%%
%%   A JSON body may be sent. It accepts the following:
%% ```
%%   { "schema" : SchemaName,
%%     "n_val" : N}
%% '''
%%   If no schema is given, it defaults to "_yz_default".
%%
%%
%% `DELETE /search/index/Index'
%%
%%   Deletes the index with the given index name.
%%

-module(yz_wm_index).
-compile(export_all).
-include("yokozuna.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(ctx, {index_name :: index_name() | undefined,    %% name the index
              props :: proplist(),                       %% properties of the body
              method :: atom(),                          %% HTTP method for the request
              ring :: ring(),                            %% Ring data
              security,                                  %% security context
              timeout :: non_neg_integer() | undefined |
                         infinity
             }).

-type context() :: #ctx{}.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return the list of routes provided by this resource.
routes() ->
    [{["search", "index", index], yz_wm_index, []},
     {["search", "index"], yz_wm_index, []}].

%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_Props) ->
    {ok, #ctx{}}.

%% NOTE: Need to grab the ring once at beginning of request because it
%%       could change as this request is being serviced.
service_available(RD, Ctx=#ctx{}) ->
    IndexName = case wrq:path_info(index, RD) of
                    undefined -> undefined;
                    V -> list_to_binary(mochiweb_util:unquote(V))
                end,
    {true,
     RD,
     Ctx#ctx{
       method=wrq:method(RD),
       index_name=IndexName,
       props=decode_json(wrq:req_body(RD)),
       ring=yz_misc:get_ring(transformed)
      }
    }.

allowed_methods(RD, S) ->
    Methods = ['GET', 'PUT', 'DELETE'],
    {Methods, RD, S}.

content_types_provided(RD, S) ->
    Types = [{"application/json", read_index}],
    {Types, RD, S}.

content_types_accepted(RD, S) ->
    Types = [{"application/json", create_index},
             {"application/octet-stream", create_index}],
    {Types, RD, S}.

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
            PermAndResource = {?YZ_SECURITY_ADMIN_PERM, ?YZ_SECURITY_INDEX},
            Res = riak_core_security:check_permission(PermAndResource, Security),
            case Res of
                {false, Error, _} ->
                    {true, wrq:append_to_resp_body(Error, RD), Ctx};
                {true, _} ->
                    {false, RD, Ctx}
            end
    end.

% Responsed to a DELETE request by removing the
% given index, and returning a 2xx code if successful
delete_resource(RD, S) ->
    IndexName = S#ctx.index_name,
    case exists(IndexName) of
        true  ->
            case yz_index:associated_buckets(IndexName, S#ctx.ring) of
                [] ->
                    ok = yz_index:remove(IndexName),
                    {true, RD, S};
                Buckets ->
                    Msg = "Can't delete index with associate buckets ~p",
                    text_response({halt,409}, Msg, [Buckets], RD, S)
            end;
        false ->
            {true, RD, S}
    end.

%% @doc Responds to a PUT request by creating an index and setting the
%%      index flag for the "index" name given in the route. Returns 204 if
%%      created. Returns a 202 if not completed withint the set timeout.
%%      Returns a 400 error if the schema does not exist, the index name
%%      contains an invalid char, or a bad n_val is given or if a request
%%      doesn't complete within a set timeout.
create_index(RD, S) ->
    IndexName = S#ctx.index_name,
    BodyProps = S#ctx.props,
    Timeout =
        case S#ctx.timeout of
            undefined -> app_helper:get_env(
                          ?YZ_APP_NAME,
                          index_put_timeout_ms,
                          ?DEFAULT_IDX_CREATE_TIMEOUT);
            Set -> Set
        end,

    SchemaName = proplists:get_value(<<"schema">>, BodyProps,
                                     ?YZ_DEFAULT_SCHEMA_NAME),
    NVal = proplists:get_value(<<"n_val">>, BodyProps, undefined),
    case maybe_create_index(IndexName, SchemaName, NVal, Timeout) of
        ok ->
            {<<>>, RD, S};
        {error, index_not_created_within_timeout} ->
            Msg = "Index ~s not created on all the nodes within ~p ms timeout",
            text_response({halt, 202}, Msg, [IndexName, Timeout], RD, S);
        {error, schema_not_found} ->
            Msg = "Cannot create index because schema ~s was not found~n",
            text_response({halt, 500}, Msg, [SchemaName], RD, S);
        {error, bad_n_val} ->
            Msg = "Bad n_val given ~p~n",
            text_response({halt, 400}, Msg, [NVal], RD, S);
        {error, invalid_name} ->
            Msg = "Invalid character in index name ~s~n",
            text_response({halt, 400}, Msg, [IndexName], RD, S);
        {error, core_error_on_index_creation, Error} ->
            text_response({halt, 400}, binary_to_list(Error), [], RD, S);
        {error, Error} ->
            Msg = "Error in creating index ~s: ~p~n",
            text_response({halt, 400}, Msg, [IndexName, Error], RD, S)
    end.

%% Responds to a GET request by returning index info for
%% the given index as a JSON response.
read_index(RD, S) ->
    case S#ctx.index_name of
        undefined  ->
            Indexes = yz_index:get_indexes_from_meta(),
            Details = [index_body(IndexName)
                || IndexName <- Indexes, yz_index:exists(IndexName)];
        IndexName ->
            Details = index_body(IndexName)
    end,
    {mochijson2:encode(Details), RD, S}.

text_response(Result, Message, Data, RD, S) ->
    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
    RD2 = wrq:append_to_response_body(io_lib:format(Message, Data), RD1),
    {Result, RD2, S}.

schema_exists_response(RD, S) ->
    Name = proplists:get_value(<<"schema">>, S#ctx.props, ?YZ_DEFAULT_SCHEMA_NAME),
    case yz_schema:exists(Name) of
        true  -> malformed_timeout_param(RD, S);
        false ->
            text_response(true, "Schema ~s does not exist~n",
                [Name], RD, S)
    end.

malformed_request(RD, S) when S#ctx.method =:= 'PUT' ->
    case S#ctx.index_name of
        undefined -> {{halt, 404}, RD, S};
        _ ->
            CT = wrq:get_req_header("Content-Type", RD),
            case CT =:= undefined andalso (not(S#ctx.props =:= [])) of
                true ->
                    text_response(true, "Missing Content-Type request header~n", [], RD, S);
                false  ->
                    schema_exists_response(RD, S)
            end
    end;
malformed_request(RD, S) when S#ctx.method =:= 'DELETE' ->
    case S#ctx.index_name of
        undefined -> {{halt, 404}, RD, S};
        _ -> {false, RD, S}
    end;
malformed_request(RD, S) ->
    IndexName = S#ctx.index_name,
    case IndexName of
      undefined -> {false, RD, S};
      _ ->
          case exists(IndexName) of
              true -> {false, RD, S};
              _ -> text_response({halt, 404}, "not found~n", [], RD, S)
          end
    end.

-spec malformed_timeout_param(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Check that the timeout parameter is are a
%%      string-encoded integer.  Store the integer value
%%      in context() if so.
malformed_timeout_param(RD, S) ->
    case wrq:get_qs_value("timeout", none, RD) of
        none ->
            {false, RD, S};
        TimeoutStr ->
            try
                Timeout = list_to_integer(TimeoutStr),
                {false, RD, S#ctx{timeout=Timeout}}
            catch
                error:badarg ->
                    {true,
                     wrq:append_to_resp_body(io_lib:format("Bad timeout "
                                                           "value ~p. "
                                                           "Could not be converted"
                                                           "to an integer value.~n",
                                                           [TimeoutStr]),
                                             wrq:set_resp_header(?HEAD_CTYPE,
                                                                 "text/plain", RD)),
                     S}
            end
    end.

%%%===================================================================
%%% Private
%%%===================================================================

%% accepts a string and attempt to parse it into json
decode_json(RDBody) ->
    case (RDBody == <<>>) or (RDBody == []) of
      true  -> [];
      false ->
          case mochijson2:decode(RDBody) of
              {struct, BodyData} -> BodyData;
              _ -> []
          end
    end.

-spec exists(undefined | index_name()) -> boolean().
exists(undefined) ->
    false;
exists(IndexName) ->
    yz_index:exists(IndexName).

%% @private
-spec index_body(index_name()) -> {struct, [{string(), binary()}]}.
index_body(IndexName) ->
    Info = yz_index:get_index_info(IndexName),
    SchemaName = yz_index:schema_name(Info),
    NVal = yz_index:get_n_val(Info),
    {struct, [
              {"name", IndexName},
              {"n_val", NVal},
              {"schema", SchemaName}
    ]}.

-spec maybe_create_index(index_name(), schema_name(), n() | undefined,
                         timeout()) -> yz_index:create_index_return().
maybe_create_index(IndexName, SchemaName, NVal, Timeout) ->
    case exists(IndexName) of
        true  ->
            ok;
        false ->
            case NVal of
                undefined ->
                    yz_index:create(IndexName, SchemaName, undefined, Timeout);
                Int when is_integer(Int), Int > 0 ->
                    yz_index:create(IndexName, SchemaName, NVal, Timeout);
                _ ->
                    {error, bad_n_val}
            end
    end.
