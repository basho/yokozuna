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
%% GET /yz/index
%%
%%   Get information about every index in JSON format.
%%   Currently the same information as /yz/index/Index,
%%   but as an array of JSON objects.
%%
%% GET /yz/index/Index
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
%% PUT /yz/index/Index
%%
%%   Creates a new index with the given name, and also creates
%%   a post commit hook to a bucket of the same name.
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
%% DELETE /yz/index/Index
%%
%%   Deletes the index with the given index name.
%%

-module(yz_wm_index).
-compile(export_all).
-include("yokozuna.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(ctx, {index_name :: string(), % name the index
              props :: proplist(),    % properties of the body
              method :: atom(),       % HTTP method for the request
              ring :: ring()
             }).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return the list of routes provided by this resource.
routes() ->
    [{["yz", "index", index], yz_wm_index, []},
     {["yz", "index"], yz_wm_index, []}].


%%%===================================================================
%%% Callbacks
%%%===================================================================

init(_Props) ->
    {ok, #ctx{}}.

%% NOTE: Need to grab the ring once at beginning of request because it
%%       could change as this request is being serviced.
service_available(RD, Ctx=#ctx{}) ->
    {true,
     RD,
     Ctx#ctx{
       method=wrq:method(RD),
       index_name=wrq:path_info(index, RD),
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

% Responsed to a DELETE request by removing the
% given index, and returning a 2xx code if successful
delete_resource(RD, S) ->
    IndexName = S#ctx.index_name,
    case yz_index:exists(IndexName) of
        true  ->
            case associated_buckets(IndexName, S#ctx.ring) of
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
        {"name", list_to_binary(IndexName)},
        {"bucket", list_to_binary(IndexName)},
        {"schema", SchemaName}
    ]}.


text_response(Result, Message, Data, RD, S) ->
    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
    RD2 = wrq:append_to_response_body(io_lib:format(Message, Data), RD1),
    {Result, RD2, S}.

schema_exists_response(RD, S) ->
    Name = proplists:get_value(<<"schema">>, S#ctx.props, ?YZ_DEFAULT_SCHEMA_NAME),
    case yz_schema:exists(Name) of
        true  -> {false, RD, S};
        false ->
            text_response(true, "Schema ~p does not exist~n",
                [binary_to_list(Name)], RD, S)
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
          case yz_index:exists(IndexName) of
              true -> {false, RD, S};
              _ -> text_response({halt, 404}, "not found~n", [], RD, S)
          end
    end.

%% Returns a 409 Conflict if this index already exists
is_conflict(RD, S) when S#ctx.method =:= 'PUT' ->
    IndexName = S#ctx.index_name,
    case yz_index:exists(IndexName) of
        true  ->
            {true, RD, S};
        false ->
            {false, RD, S}
    end.

%%%===================================================================
%%% Private
%%%===================================================================

-spec associated_buckets(index_name(), ring()) -> [bucket()].
associated_buckets(IndexName, Ring) ->
    AllBucketProps = riak_core_bucket:get_buckets(Ring),
    Indexes = lists:map(fun yz_kv:which_index/1, AllBucketProps),
    lists:filter(fun(I) -> I == IndexName end, Indexes).

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

-spec maybe_create_index(index_name(), schema_name()) -> ok |
                                                         {error, schema_not_found} |
                                                         {error, {rpc_fail, node(), term()}}.
maybe_create_index(IndexName, SchemaName)->
    case yz_index:exists(IndexName) of
        true  ->
            ok;
        false ->
            yz_index:create(IndexName, SchemaName)
    end.
