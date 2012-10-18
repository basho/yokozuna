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

-module(yz_wm_index).
-compile(export_all).
-include("yokozuna.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return the list of routes provided by this resource.
routes() ->
    [{["yz", "index", index], yz_wm_index, []}].


%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_) ->
    {ok, none}.

allowed_methods(RD, S) ->
    Methods = ['GET', 'PUT', 'DELETE'],
    {Methods, RD, S}.

content_types_provided(RD, S) ->
    Types = [{"application/json", read_index}],
    {Types, RD, S}.

content_types_accepted(RD, S) ->
    Types = [{"application/json", create_index}],
    {Types, RD, S}.

% Responsed to a DELETE request by removing the
% given index, and returning a 2xx code if successful
delete_resource(RD, S) ->
    ?INFO("DELETE~n", []),
    IndexName = wrq:path_info(index, RD),
    case yz_index:exists(IndexName) of
        true  ->
            delete_index(IndexName),
            {true, RD, S};
        false -> {false, RD, S}
    end.

%% Responds to a PUT request by creating an index
%% and hook for the "index" name given in the route
%% Returns "ok" if created, or "exists" if an index
%% of that name already exists. Returns a 500 error if
%% the given schema does not exist.
create_index(RD, S) ->
    RDBody = wrq:req_body(RD),
    case RDBody of
        "" -> SchemaName = ?YZ_DEFAULT_SCHEMA_NAME;
        _  -> SchemaName = get_schema_name(RDBody)
    end,
    case yz_schema:get(SchemaName) of
        {notfound, _} ->
            BodyResp = wrq:set_resp_header("Content-Type", "text/plain", RD),
            BodyResp2 = wrq:set_resp_body("Schema does not exist", BodyResp),
            {{halt, 500}, BodyResp2, S};
        _ ->
            ?INFO("PUT ~p~n", [SchemaName]),
            IndexName = wrq:path_info(index, RD),
            Body = create_install_index_if_exists(IndexName, SchemaName),
            BodyResp = wrq:set_resp_header("Content-Type", "text/plain", RD),
            BodyResp2 = wrq:set_resp_body(Body, BodyResp),
            {Body, BodyResp2, S}
    end.


%% Responds to a GET request by returning index info for
%% the given index as a JSON response.
read_index(RD, S) ->
    % case wrq:get_qs_value("raw_schema", RD) of
    %     undefined   -> GetRawSchema = false;
    %     "true"      -> GetRawSchema = true;
    %     _           -> GetRawSchema = false
    % end,
    IndexName = wrq:path_info(index, RD),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Info = yz_index:get_info_from_ring(Ring, IndexName),
    SchemaName = yz_index:schema_name(Info),
    % RawSchema = yz_schema:get(SchemaName),
    Body = mochijson:encode({struct, [
          {"name", IndexName},
          {"bucket", IndexName},
          {"schema", SchemaName}
        ]}),
    {Body, RD, S}.


%% Responds to a GET request by returning index info for
%% all indexes as a JSON response.
% read_indexes(RD, S) ->
%     yz_index:get_indexes_from_ring(Ring).


%% private

% Extracts the schema name from a json string.
get_schema_name(RDBody)->
    case mochijson2:decode(RDBody) of
        {struct, BodyData} -> 
            case proplists:get_value(<<"schema">>, BodyData) of
                undefined  -> ?YZ_DEFAULT_SCHEMA_NAME;
                SchemaName -> SchemaName
            end;
        _ -> ?YZ_DEFAULT_SCHEMA_NAME
    end.

% If the index exists, return "exists".
% If not, create it and return "ok"
create_install_index_if_exists(IndexName, SchemaName)->
    case yz_index:exists(IndexName) of
        true  -> "exists";
        false ->
            ok = yz_index:create(IndexName, SchemaName),
            ok = yz_kv:install_hook(list_to_binary(IndexName)),
            "ok"
    end.

% TODO: delete the index
delete_index(_IndexName)->
    true.
