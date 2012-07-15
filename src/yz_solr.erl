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

-module(yz_solr).
-compile(export_all).
-include("yokozuna.hrl").

-define(CORE_ALIASES, [{index_dir, instanceDir},
                       {cfg_file, config},
                       {java_lib_dir, "property.yz_java_lib_dir"},
                       {schema_file, schema}]).
-define(DEFAULT_URL, "http://localhost:8983/solr").
-define(DEFAULT_VCLOCK_N, 1000).

%% @doc This module provides the interface for making calls to Solr.
%%      All interaction with Solr should go through this API.

%%%===================================================================
%%% API
%%%===================================================================

commit(Core) ->
    BaseURL = base_url() ++ "/" ++ Core ++  "/update",
    XML = encode_commit(),
    Params = [{commit, true}],
    Encoded = mochiweb_util:urlencode(Params),
    URL = BaseURL ++ "?" ++ Encoded,
    Headers = [{content_type, "text/xml"}],
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, Headers, post, XML, Opts) of
        {ok, "200", _, _} -> ok;
        Err -> throw({"Failed to commit", Err})
    end.

%% @doc Perform Core related actions.
-spec core(atom(), proplists:proplist()) -> ok.
core(Action, Props) ->
    BaseURL = base_url() ++ "/admin/cores",
    Action2 = convert_action(Action),
    Params = proplists:substitute_aliases(?CORE_ALIASES,
                                          [{action, Action2}|Props]),
    Encoded = mochiweb_util:urlencode(Params),
    Opts = [{response_format, binary}],
    URL = BaseURL ++ "?" ++ Encoded,

    case ibrowse:send_req(URL, [], get, [], Opts) of
        {ok, "200", _Headers, _Body} ->
            ok;
        X ->
            throw({error_calling_solr, create_core, X})
    end.

delete(Core, DocID) ->
    BaseURL = base_url() ++ "/" ++ Core ++ "/update",
    XML = encode_delete(DocID),
    Params = [],
    Encoded = mochiweb_util:urlencode(Params),
    URL = BaseURL ++ "?" ++ Encoded,
    Headers = [{content_type, "text/xml"}],
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, Headers, post, XML, Opts) of
        {ok, "200", _, _} -> ok;
        Err -> throw({"Failed to delete doc", DocID, Err})
    end.

%% @doc Get `N' key-vclock pairs that occur before the `Before'
%%      timestamp.
-spec get_vclocks(string(), iso8601()) -> solr_vclocks().
get_vclocks(Core, Before) ->
    get_vclocks(Core, Before, none, ?DEFAULT_VCLOCK_N).

get_vclocks(Core, Before, Continue, N) when N > 0 ->
    BaseURL = base_url() ++ "/" ++ Core ++ "/entropy_data",
    Params = [{before, Before}, {wt, json}, {n, N}],
    Params2 = if Continue == none -> Params;
                 true -> [{continue, Continue}|Params]
              end,
    Encoded = mochiweb_util:urlencode(Params2),
    Opts = [{response_format, binary}],
    URL = BaseURL ++ "?" ++ Encoded,
    case ibrowse:send_req(URL, [], get, [], Opts) of
        {ok, "200", _Headers, Body} ->
            R = mochijson2:decode(Body),
            More = json_get_key(<<"more">>, R),
            Continuation = get_continuation(More, R),
            Pairs = get_pairs(R),
            make_solr_vclocks(More, Continuation, Pairs);
        X ->
            {error, X}
    end.

%% @doc Index the given `Docs'.
index(Core, Docs) ->
    BaseURL = base_url() ++ "/" ++ Core ++ "/update",
    Doc = {add, [], lists:map(fun encode_doc/1, Docs)},
    XML = xmerl:export_simple([Doc], xmerl_xml),
    Params = [],
    Encoded = mochiweb_util:urlencode(Params),
    URL = BaseURL ++ "?" ++ Encoded,
    Headers = [{content_type, "text/xml"}],
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, Headers, post, XML, Opts) of
        {ok, "200", _, _} -> ok;
        Err -> throw({"Failed to index docs", Docs, Err})
    end.

%% @doc Return boolean based on ping response from Solr.
-spec ping(string()) -> boolean().
ping(Core) ->
    URL = base_url() ++ "/" ++ Core ++ "/admin/ping",
    case ibrowse:send_req(URL, [], get) of
        {ok, "200", _, _} -> true;
        _ -> false
    end.

port() ->
    app_helper:get_env(?YZ_APP_NAME, solr_port, ?YZ_DEFAULT_SOLR_PORT).

search(Core, Params, Mapping) ->
    {Nodes, FilterPairs} = yz_cover:plan(Core),
    HostPorts = [proplists:get_value(Node, Mapping) || Node <- Nodes],
    ShardFrags = [shard_frag(Core, HostPort) || HostPort <- HostPorts],
    ShardFrags2 = string:join(ShardFrags, ","),
    FQ = build_fq(FilterPairs),
    BaseURL = base_url() ++ "/" ++ Core ++ "/select",
    Params2 = Params ++ [{fq, FQ}],
    Encoded = mochiweb_util:urlencode(Params2),
    %% NOTE: For some reason ShardFrags2 breaks urlencode so add it
    %%       manually
    URL = BaseURL ++ "?shards=" ++ ShardFrags2 ++ "&" ++ Encoded,
    Headers = [],
    Body = [],
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, Headers, get, Body, Opts) of
        {ok, "200", _, Resp} -> Resp;
        Err -> throw({"Failed to search", URL, Err})
    end.

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Get the base URL.
base_url() ->
    "http://localhost:" ++ port() ++ "/solr".

build_fq(Partitions) ->
    Fields = [filter_to_str(P) || P <- Partitions],
    string:join(Fields, " OR ").

filter_to_str({Partition, all}) ->
    "_pn:" ++ integer_to_list(Partition);
filter_to_str({Partition, FPFilter}) ->
    PNQ = "_pn:" ++ integer_to_list(Partition),
    FPQ = string:join(lists:map(fun integer_to_list/1, FPFilter), " OR "),
    FPQ2 = "_fpn:" ++ FPQ,
    "(" ++ PNQ ++ " AND (" ++ FPQ2 ++ "))".


convert_action(create) -> "CREATE".

%% TODO: Encoding functions copied from esolr, redo this.
encode_commit() ->
	xmerl:export_simple([{commit, []}], xmerl_xml).

encode_delete({id,Id})->
	xmerl:export_simple([{delete, [], [{id, [], [Id]}]}], xmerl_xml).

encode_doc({doc, Fields}) ->
	{doc, [], lists:map(fun encode_field/1,Fields)};

encode_doc({doc, Boost, Fields}) ->
	{doc, [{boost, Boost}], lists:map(fun encode_field/1, Fields)}.

encode_field({Name, Value}) when is_binary(Value)->
	{field, [{name,Name}], [[Value]]};

encode_field({Name,Value}) ->
	{field, [{name,Name}], [Value]};

encode_field({Name,Value,Boost}) when is_binary(Value)->
	{field, [{name,Name}, {boost,Boost}], [[Value]]};

encode_field({Name,Value,Boost}) ->
	{field, [{name,Name}, {boost,Boost}], [Value]}.

%% @doc Get the continuation value if there is one.
get_continuation(false, _R) ->
    none;
get_continuation(true, R) ->
    json_get_key(<<"continuation">>, R).

get_pairs(R) ->
    Docs = json_get_key(<<"docs">>, get_response(R)),
    [to_pair(DocStruct) || DocStruct <- Docs].

to_pair({struct, [{_,DocId},{_,Base64VClock}]}) ->
    {DocId, Base64VClock}.

get_response(R) ->
    json_get_key(<<"response">>, R).

%% @doc Given a "struct" created by `mochijson2:decode' get the given
%%      `Key' or throw if not found.
json_get_key(Key, {struct, PL}) ->
    case proplists:get_value(Key, PL) of
        undefined -> {error, not_found, Key, PL};
        Val -> Val
    end;
json_get_key(_Key, Term) ->
    throw({error, "json_get_key: Term not a struct", Term}).

make_solr_vclocks(More, Continuation, Pairs) ->
    #solr_vclocks{more=More, continuation=Continuation, pairs=Pairs}.

shard_frag(Core, {Host, Port}) ->
    Host ++ ":" ++ Port ++ "/solr/" ++ Core.
