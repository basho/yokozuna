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
                       {schema_file, schema},
                       {delete_instance, deleteInstanceDir}]).
-define(FIELD_ALIASES, [{continuation, continue},
                        {limit,n}]).
-define(DEFAULT_URL, "http://localhost:8983/solr").
-define(DEFAULT_VCLOCK_N, 1000).
-define(QUERY(Str), {struct, [{'query', Str}]}).
-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).

%% @doc This module provides the interface for making calls to Solr.
%%      All interaction with Solr should go through this API.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Create a mapping from the `Nodes' hostname to the port which
%% Solr is listening on.  The resulting list could be smaller than the
%% input in the case that the port cannot be determined for one or
%% more nodes.
-spec build_mapping([node()]) -> [{node(), {string(), string()}}].
build_mapping(Nodes) ->
    [{Node, HP} || {Node, HP={_,P}} <- [{Node, host_port(Node)}
                                        || Node <- Nodes],
                   P /= unknown].

-spec build_partition_delete_query(ordset(lp())) -> term().
build_partition_delete_query(LPartitions) ->
    Deletes = [{delete, ?QUERY(<<?YZ_PN_FIELD_S, ":", (?INT_TO_BIN(LP))/binary>>)}
               || LP <- LPartitions],
    mochijson2:encode({struct, Deletes}).

-spec commit(index_name()) -> ok.
commit(Core) ->
    JSON = encode_commit(),
    Params = [{commit, true}],
    Encoded = mochiweb_util:urlencode(Params),
    URL = ?FMT("~s/~s/update?~s", [base_url(), Core, Encoded]),
    Headers = [{content_type, "application/json"}],
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, Headers, post, JSON, Opts) of
        {ok, "200", _, _} -> ok;
        Err -> throw({"Failed to commit", Err})
    end.

%% @doc Perform Core related actions.
-spec core(atom(), proplists:proplist()) -> {ok, any(), any()} | {error, term()}.
core(Action, Props) ->
    core(Action, Props, 5000).

core(Action, Props, Timeout) ->
    BaseURL = base_url() ++ "/admin/cores",
    Action2 = convert_action(Action),
    Params = proplists:substitute_aliases(?CORE_ALIASES,
                                          [{action, Action2}|Props]),
    Encoded = mochiweb_util:urlencode(Params),
    Opts = [{response_format, binary}],
    URL = BaseURL ++ "?" ++ Encoded,

    case ibrowse:send_req(URL, [], get, [], Opts, Timeout) of
        {ok, "200", Headers, Body} ->
            {ok, Headers, Body};
        X ->
            {error, X}
    end.

-spec cores() -> {ok, ordset(index_name())} | {error, term()}.
cores() ->
    case yz_solr:core(status, [{wt,json}]) of
        {ok, _, Body} ->
            {struct, Status} = kvc:path([<<"status">>], mochijson2:decode(Body)),
            Cores = ordsets:from_list([Name || {Name, _} <- Status]),
            {ok, Cores};
        {error,_} = Err ->
            Err
    end.

%% @doc Perform the delete `Ops' against the `Index'.
-spec delete(index_name(), [delete_op()]) -> ok | {error, term()}.
delete(Index, Ops) ->
    JSON = mochijson2:encode({struct, [{delete, encode_delete(Op)} || Op <- Ops]}),
    URL = ?FMT("~s/~s/update", [base_url(), Index]),
    Headers = [{content_type, "application/json"}],
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, Headers, post, JSON, Opts) of
        {ok, "200", _, _} -> ok;
        Err -> {error, Err}
    end.

%% @doc Get slice of entropy data.  Entropy data is used to build
%%      hashtrees for active anti-entropy.  This is meant to be called
%%      in an iterative fashion in order to page through the entropy
%%      data.
%%
%%  `Core' - The core to get entropy data for.
%%
%%  `Filter' - The list of constraints to filter out entropy
%%             data.
%%
%%    `before' - An ios8601 datetime, return data for docs written
%%               before and including this moment.
%%
%%    `continuation' - An opaque value used to continue where a
%%                     previous return left off.
%%
%%    `limit' - The maximum number of entries to return.
%%
%%    `partition' - Return entries for specific logical partition.
%%
%%  `ED' - An entropy data record containing list of entries and
%%         continuation value.
-spec entropy_data(index_name(), ed_filter()) ->
                          ED::entropy_data() | {error, term()}.
entropy_data(Core, Filter) ->
    Params = [{wt, json}|Filter] -- [{continuation, none}],
    Params2 = proplists:substitute_aliases(?FIELD_ALIASES, Params),
    Opts = [{response_format, binary}],
    URL = ?FMT("~s/~s/entropy_data?~s",
               [base_url(), Core, mochiweb_util:urlencode(Params2)]),
    case ibrowse:send_req(URL, [], get, [], Opts) of
        {ok, "200", _Headers, Body} ->
            R = mochijson2:decode(Body),
            More = kvc:path([<<"more">>], R),
            Continuation = get_continuation(More, R),
            Pairs = get_pairs(R),
            make_ed(More, Continuation, Pairs);
        X ->
            {error, X}
    end.

%% @doc Index the given `Docs'.
index(Core, Docs) ->
    index(Core, Docs, []).

-spec index(index_name(), list(), [delete_op()]) -> ok.
index(Core, Docs, DelOps) ->
    Ops = {struct,
           [{delete, encode_delete(Op)} || Op <- DelOps] ++
               [{add, encode_doc(D)} || D <- Docs]},
    JSON = mochijson2:encode(Ops),
    URL = ?FMT("~s/~s/update", [base_url(), Core]),
    Headers = [{content_type, "application/json"}],
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, Headers, post, JSON, Opts) of
        {ok, "200", _, _} -> ok;
        Err -> throw({"Failed to index docs", Ops, Err})
    end.

prepare_json(Docs) ->
    Content = {struct, [{add, encode_doc(D)} || D <- Docs]},
    mochijson2:encode(Content).

%% @doc Return the set of unique partitions stored on this node.
-spec partition_list(index_name()) -> {ok, Resp::binary()} | {error, term()}.
partition_list(Core) ->
    Params = [{q, "*:*"},
              {facet, "on"},
              {"facet.mincount", "1"},
              {"facet.field", ?YZ_PN_FIELD_S},
              {wt, "json"}],
    Encoded = mochiweb_util:urlencode(Params),
    URL = ?FMT("~s/~s/select?~s", [base_url(), Core, Encoded]),
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, [], get, [], Opts) of
        {ok, "200", _, Resp} -> {ok, Resp};
        Err -> {error, Err}
    end.

%% @doc Return boolean based on ping response from Solr.
-spec ping(index_name()) -> boolean().
ping(Core) ->
    URL = ?FMT("~s/~s/admin/ping", [base_url(), Core]),
    case ibrowse:send_req(URL, [], get) of
        {ok, "200", _, _} -> true;
        _ -> false
    end.

-spec port() -> non_neg_integer().
port() ->
    app_helper:get_env(?YZ_APP_NAME, solr_port, ?YZ_DEFAULT_SOLR_PORT).

jmx_port() ->
    app_helper:get_env(?YZ_APP_NAME, solr_jmx_port, undefined).

dist_search(Core, Params) ->
    dist_search(Core, [], Params).

dist_search(Core, Headers, Params) ->
    Plan = yz_cover:plan(Core),
    case Plan of
        {error, _} = Err ->
            Err;
        {Nodes, FilterPairs, Mapping} ->
            HostPorts = [proplists:get_value(Node, Mapping) || Node <- Nodes],
            ShardFrags = [shard_frag(Core, HostPort) || HostPort <- HostPorts],
            ShardFrags2 = string:join(ShardFrags, ","),
            FQ = build_fq(FilterPairs),
            Params2 = Params ++ [{shards, ShardFrags2}, {fq, FQ}],
            search(Core, Headers, Params2)
    end.

search(Core, Headers, Params) ->
    Body = mochiweb_util:urlencode(Params),
    URL = ?FMT("~s/~s/select", [base_url(), Core]),
    Headers2 = [{content_type, "application/x-www-form-urlencoded"}|Headers],
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, Headers2, post, Body, Opts) of
        {ok, "200", RHeaders, Resp} -> {RHeaders, Resp};
        {ok, "404", _, _} -> throw(not_found);
        {ok, CodeStr, _, Err} ->
            {Code, _} = string:to_integer(CodeStr),
            throw({solr_error, {Code, URL, Err}});
        Err -> throw({"Failed to search", URL, Err})
    end.


%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Get the base URL.
base_url() ->
    "http://localhost:" ++ integer_to_list(port()) ++ "/solr".

build_fq(Partitions) ->
    GroupedByNode = yz_misc:group_by(Partitions, fun group_by_node/1),
    Fields = [group_to_str(G) || G <- GroupedByNode],
    string:join(Fields, " OR ").

%% @private
%%
%% @doc Get hostname and Solr port for `Node'.  Return `unknown' for
%% the port if the RPC fails.
-spec host_port(node()) -> {string(), string() | unknown}.
host_port(Node) ->
    case rpc:call(Node, yz_solr, port, [], 1000) of
        {badrpc, Reason} ->
            ?DEBUG("error retrieving Solr port ~p ~p", [Node, Reason]),
            {yz_misc:hostname(Node), unknown};
        Port when is_integer(Port) ->
            {yz_misc:hostname(Node), integer_to_list(Port)}
    end.

group_by_node({{Partition, Owner}, all}) ->
    {Owner, Partition};
group_by_node({{Partition, Owner}, FPFilter}) ->
    {Owner, {Partition, FPFilter}}.

group_to_str({Owner, Partitions}) ->
    OwnerQ = ?YZ_NODE_FIELD_S ++ ":" ++ atom_to_list(Owner),
    "(" ++ OwnerQ ++ " AND " ++ "(" ++ partitions_to_str(Partitions) ++ "))".

partitions_to_str(Partitions) ->
    F = fun({Partition, FPFilter}) ->
                PNQ = pn_str(Partition),
                FPQ = string:join(lists:map(fun fpn_str/1, FPFilter), " OR "),
                "(" ++ PNQ ++ " AND " ++ "(" ++ FPQ ++ "))";
           (Partition) ->
                pn_str(Partition)
        end,
    string:join(lists:map(F, Partitions), " OR ").

pn_str(Partition) ->
    ?YZ_PN_FIELD_S ++ ":" ++ integer_to_list(Partition).

fpn_str(FPN) ->
    ?YZ_FPN_FIELD_S ++ ":" ++ integer_to_list(FPN).

convert_action(create) -> "CREATE";
convert_action(status) -> "STATUS";
convert_action(remove) -> "UNLOAD";
convert_action(reload) -> "RELOAD".

encode_commit() ->
    <<"{}">>.

%% @private
%%
%% @doc Encode a delete operation into a mochijson2 compatiable term.
-spec encode_delete(delete_op()) -> term().
encode_delete({key,Key}) ->
    Query = ?YZ_RK_FIELD_S ++ ":" ++ ibrowse_lib:url_encode(binary_to_list(Key)),
    ?QUERY(list_to_binary(Query));
encode_delete({siblings,Key}) ->
    Query = ?YZ_RK_FIELD_S ++ ":" ++ ibrowse_lib:url_encode(binary_to_list(Key)) ++ " AND " ++ ?YZ_VTAG_FIELD_S ++ ":[* TO *]",
    ?QUERY(list_to_binary(Query));
encode_delete({'query', Query}) ->
    ?QUERY(Query);
encode_delete({id, Id}) ->
    %% NOTE: Solr uses the name `id' to represent the `uniqueKey'
    %%       field of the schema.  Thus `id' must be passed, not
    %%       `YZ_ID_FIELD'.
    {struct, [{id, Id}]}.

encode_doc({doc, Fields}) ->
    {struct, [{doc, lists:map(fun encode_field/1,Fields)}]}.

encode_field({Name,Value}) when is_list(Value) ->
    {Name, list_to_binary(Value)};
encode_field({Name,Value}) ->
    {Name, Value}.

%% @doc Get the continuation value if there is one.
get_continuation(false, _R) ->
    none;
get_continuation(true, R) ->
    kvc:path([<<"continuation">>], R).

get_pairs(R) ->
    Docs = kvc:path([<<"response">>, <<"docs">>], R),
    [to_pair(DocStruct) || DocStruct <- Docs].

to_pair({struct, [{_,_Vsn},{_,BType},{_,BName},{_,Key},{_,Base64Hash}]}) ->
    {{{BType, BName},Key}, base64:decode(Base64Hash)}.

get_doc_pairs(Resp) ->
    Docs = kvc:path([<<"docs">>], Resp),
    [to_doc_pairs(DocStruct) || DocStruct <- Docs].

to_doc_pairs({struct, Values}) ->
    Values.

-spec get_response(term()) -> term().
get_response(R) ->
    kvc:path([<<"response">>], R).

make_ed(More, Continuation, Pairs) ->
    #entropy_data{more=More, continuation=Continuation, pairs=Pairs}.

-spec shard_frag(index_name(), {string(), string()}) -> string().
shard_frag(Core, {Host, Port}) ->
    ?FMT("~s:~s/solr/~s", [Host, Port, Core]).
