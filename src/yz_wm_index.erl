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
%% GET /search/index
%%
%%   Get information about every index in JSON format.
%%   Currently the same information as /search/index/Index,
%%   but as an array of JSON objects.
%%
%% GET /search/index/Index
%%
%%   Gets information about a specific index in JSON format.
%%   Returns the following information:
%%
%%   {
%%      "name"  : IndexName,
%%      "schema": SchemaName
%%   }
%%
%%   `IndexName' is the same value passed into the URL.
%%
%%   `Schema' is the name of the schema associate with this
%%   index. That schema file must already be installed on the server.
%%   Defaults to "_yz_default".
%%
%% PUT /search/index/Index
%%
%%   Creates a new index with the given name.
%%
%%   A PUT request requires this header:
%%     Content-Type: application/json
%%
%%   A JSON body may be sent. It currently only accepts
%%
%%   { "schema" : SchemaName }
%%
%%   If no schema is given, it defaults to "_yz_default".
%%
%%   Returns a '409 Conflict' code if the index already exists.
%%
%% DELETE /search/index/Index
%%
%%   Deletes the index with the given index name.
%%
%% POST /search/index/Index
%%
%%   Allow miscellaneous actions to be performed on `Index'. Currently
%%   only `reload' is supported. This resource accepts a JSON body
%%   describing the action to be taken. The JSON object must have an
%%   `action' member which determines which action will be taken along
%%   with 0 or many additional members which act as arguments to the
%%   action.
%%
%%   RELOAD
%%   ------
%%
%%   This is a cluster-wide, blocking action. The call will not return
%%   until all nodes have reloaded the index or failed trying to do
%%   so. The return value should be checked for errors and the action
%%   replayed if errors occurred.
%%
%%   `reload_schema' - (Optional) - A boolean dictating if the
%%   associated schema should be reloaded. Defaults to `true'. This is
%%   used to load the latest schema after a modification such as
%%   adding a field.
%%
%%   `timeout' - (Optional) - A number representing the number of
%%   milliseconds to wait for completion. Default is 5000.
%%
%%   { "action":"reload",
%%     [ "reload_schema": true | false ],
%%     [ "timeout":5000 ] }
%%
%%   The response will be a JSON object as well. The status code will
%%   be `200' to indicate success or `500' to indicate failure. In
%%   both cases the JSON object will have two members: `success' and
%%   `fail'.
%%
%%   { "success": ["riak@node1", "riak@node2", "riak@node3", ...],
%%     "fail": [{"node":"riak@node4", "reason":"down"}] }
%%

-module(yz_wm_index).
-compile(export_all).
-include("yokozuna.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(ctx, {index_name :: index_name() | undefined, %% name the index
              props :: proplist(),    %% properties of the body
              method :: atom(),       %% HTTP method for the request
              ring :: ring(),         %% Ring data
              security                %% security context
             }).

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
                    V -> list_to_binary(V)
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
    Methods = ['GET', 'PUT', 'POST', 'DELETE'],
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
                    {true, wrq:append_to_resp_body(list_to_binary(Error), RD), Ctx};
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

%% Responds to a PUT request by creating an index and setting the
%% index flag for the "index" name given in the route. Returns 204 if
%% created, or 409 if an index of that name already exists. Returns a
%% 400 error if the schema does not exist.
create_index(RD, S) ->
    IndexName = S#ctx.index_name,
    BodyProps = S#ctx.props,
    SchemaName = proplists:get_value(<<"schema">>, BodyProps, ?YZ_DEFAULT_SCHEMA_NAME),
    case maybe_create_index(IndexName, SchemaName) of
        ok ->
            {<<>>, RD, S};
        {error, {rpc_fail, Claimant, _}} ->
            Msg = "Cannot create index while claimant node ~p is down~n",
            text_response({halt, 500}, Msg, [Claimant], RD, S)
    end.


%% Responds to a GET request by returning index info for
%% the given index as a JSON response.
read_index(RD, S) ->
    Ring = S#ctx.ring,
    case S#ctx.index_name of
        undefined  ->
            Indexes = yz_index:get_indexes_from_ring(Ring),
            Details = [index_body(Ring, IndexName)
              || IndexName <- orddict:fetch_keys(Indexes)];
        IndexName ->
            Details = index_body(Ring, IndexName)
    end,
    {mochijson2:encode(Details), RD, S}.

index_body(Ring, IndexName) ->
    Info = yz_index:get_info_from_ring(Ring, IndexName),
    SchemaName = yz_index:schema_name(Info),
    {struct, [
        {"name", IndexName},
        {"schema", SchemaName}
    ]}.

-spec json_response({struct, list()}, term()) -> term().
json_response(MochiStruct, RD) ->
    RD1 = wrq:set_resp_header("Content-Type", "application/json", RD),
    wrq:append_to_response_body(mochijson2:encode(MochiStruct), RD1).

text_response(Result, Message, Data, RD, S) ->
    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
    RD2 = wrq:append_to_response_body(io_lib:format(Message, Data), RD1),
    {Result, RD2, S}.

schema_exists_response(RD, S) ->
    Name = proplists:get_value(<<"schema">>, S#ctx.props, ?YZ_DEFAULT_SCHEMA_NAME),
    case yz_schema:exists(Name) of
        true  -> {false, RD, S};
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
%% TODO: Need to check index name on POST
malformed_request(RD, S) ->
    IndexName = S#ctx.index_name,
    case IndexName of
      undefined ->
            {false, RD, S};
      _ ->
          case exists(IndexName) of
              true -> {false, RD, S};
              _ ->
                  %% TODO: this should be 400 because a non-existant index was passed
                  text_response({halt, 404}, "not found~n", [], RD, S)
          end
    end.

%% Returns a 409 Conflict if this index already exists
is_conflict(RD, S) when S#ctx.method =:= 'PUT' ->
    IndexName = S#ctx.index_name,
    case exists(IndexName) of
        true  ->
            {true, RD, S};
        false ->
            {false, RD, S}
    end.

process_post(RD, S) ->
    CT = wrq:get_req_header("content-type", RD),
    case CT == "application/json" of
        true ->
            Body = wrq:req_body(RD),
            case process_post_body(IndexName, Body) of
                {ok, MochiStruct} ->
                    RD2 = json_response(MochiStruct, RD),
                    {true, RD2, S};
                {error, MochiStruct} ->
                    RD2 = json_response(MochiStruct, RD),
                    {{halt,500}, RD2, S}
            end;
        false ->
            %% TODO: set error myself, this looks like garbage
            {{error, "Must use content-type of application/json"}, RD, S}
    end.

%% %% @doc Convert the binary to lowercase. Assumes ASCII only.
%% -spec binary_lower_case(binary()) -> binary().
%% binary_to_lower(Binary) ->
%%     list_to_binary(string:to_lower(binary_to_list(Binary))).

-spec process_post_body(index_name(), binary()) -> {ok | error, {struct, list()}}.
process_post_body(IndexName, Body) ->
    Decoded = mochijson2:decode(Body),
    Action = kvc:path([<<"action">>], Decoded),
    process_post_action(IndexName, Action, Decoded).

-spec process_post_action(index_name(), binary(), {struct, list()}) ->
                                 {ok | error, {struct, list()}}.
process_post_action(IndexName, <<"reload">>, Decoded) ->
    Opts1 = maybe_add(<<"reload_schema">>, Decoded, []),
    Opts2 = maybe_add(<<"timeout">>, Decoded, Opts1),
    case yz_index:reload_index(IndexName, Opts2) of
        {ok, Nodes} ->
            S = {struct, [{<<"success">>, Nodes}]},
            {ok, S};
        {error, Resps} ->
            FailArr = [{struct, [{<<"node">>, N}, {<<"reason">>, R}]}
                       || {N,{error,R}} <- Resps],
            S = {struct, [<<"fail">>, FailArr]},
            {error, S}
    end.

-spec maybe_add(binary(), {struct, list()}, [{atom(), term()}]) ->
                       [{atom(), term()}].
maybe_add(OptionName, Decoded, Opts) ->
    case kvc:value(OptionName, Decoded, undefined) of
        undefined -> Opts;
        Val -> [{?BIN_TO_ATOM(OptionName), Val}|Opts]
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

-spec maybe_create_index(index_name(), schema_name()) -> ok |
                                                         {error, schema_not_found} |
                                                         {error, {rpc_fail, node(), term()}}.
maybe_create_index(IndexName, SchemaName)->
    case exists(IndexName) of
        true  ->
            ok;
        false ->
            yz_index:create(IndexName, SchemaName)
    end.
