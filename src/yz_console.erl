%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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
-module(yz_console).
-include("yokozuna.hrl").
-export([aae_status/1,
         switch_to_new_search/1,
         create_schema/1,
         show_schema/1,
         add_to_schema/1,
         remove_from_schema/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type field_data() :: {dynamicfield | field, list()}.

-define(LIST_TO_ATOM(L), list_to_atom(L)).
-define(LIST_TO_BINARY(L), list_to_binary(L)).
-define(FIELD_DEFAULTS, [{type, "text_general"},
                         {indexed, "true"},
                         {stored, "false"},
                         {multiValued, "true"}]).

%% @doc Print the Active Anti-Entropy status to stdout.
-spec aae_status([]) -> ok.
aae_status([]) ->
    ExchangeInfo = yz_kv:compute_exchange_info(),
    riak_kv_console:aae_exchange_status(ExchangeInfo),
    io:format("~n"),
    TreeInfo = yz_kv:compute_tree_info(),
    riak_kv_console:aae_tree_status(TreeInfo),
    io:format("~n"),
    riak_kv_console:aae_repair_status(ExchangeInfo),
    ok.

%% @doc Switch over HTTP solr route and PB handling from legacy
%% riak_search to yokozuna. This will multicall to all nodes in the
%% cluster. If any nodes are down report them to stderr and return an
%% error tuple. Once the switch is made the system cannot be switched
%% back without restarting the cluster.
-spec switch_to_new_search([]) -> ok | {error, {nodes_down, [node()]}}.
switch_to_new_search([]) ->
    {_Good, Down} = riak_core_util:rpc_every_member_ann(
                      yokozuna, switch_to_yokozuna, [], 5000),
    case Down of
        [] ->
            ok;
        _ ->
            Down2 = [atom_to_list(Node) || Node <- Down],
            DownStr = string:join(Down2, " "),
            io:format(standard_error, "The following nodes could not be reached: ~s", [DownStr]),
            {error, {nodes_down, Down}}
    end.

%% @doc Creates (and overrides) schema for name and file path.
%% riak-admin search schema create <schema name> <path to file>
-spec create_schema([string()|string()]) -> ok | error.
create_schema([Name, Path]) ->
    try
        {ok, RawSchema} = read_schema(Path),
        FMTName = ?LIST_TO_ATOM(Name),
        case yz_schema:store(?LIST_TO_BINARY(Name), RawSchema) of
            ok ->
                io:format("~p schema created~n", [FMTName]),
                ok;
            {error, _} ->
                io:format("Error creating schema ~p~n", [FMTName]),
                error
        end
    catch {fileReadError, _} ->
           error
    end.

%% @doc Shows solr schema for name passed in.
%% riak-admin search schema show <name of schema>
-spec show_schema([string()]) -> ok | error.
show_schema([Name]) ->
    FMTName = ?LIST_TO_ATOM(Name),
    case yz_schema:get(?LIST_TO_BINARY(Name)) of
        {ok, R} ->
            io:format("Schema ~p:~n~s~n", [FMTName, binary_to_list(R)]),
            ok;
        {error, notfound} ->
            io:format("Schema ~p not found~n", [FMTName]),
            error
    end.

%% @doc Adds field of <fieldname> to schema contents.
%% riak-admin search schema <name> add field|dynamicfield <fieldname> [<option>=<value> [...]]
-spec add_to_schema([string()|string()]) -> ok | error.
add_to_schema([Name,FieldType,FieldName|Options]) ->
    try
        ParsedOptions = [{?LIST_TO_ATOM(K), V} || {K, V} <- parse_options(Options)],
        Field = make_field(?LIST_TO_ATOM(FieldType), FieldName, ParsedOptions),
        %% TODO: Wrap *update_schema* in a Case to check for ok|error
        update_schema(add, Name, Field),
        ok
    catch {error, {invalid_option, Option}} ->
            io:format("Invalid Option: ~p~n", [?LIST_TO_ATOM(Option)]),
            error
    end.


%% @doc Removes field of <fieldname> from schema contents.
%% riak-admin search schema <name> remove <fieldname>
-spec remove_from_schema([string()|string()]) -> ok | error.
remove_from_schema([Name, FieldName]) ->
    %% TODO: Wrap Update in a Case to check for ok|error
    update_schema(remove, Name, FieldName),
    ok.

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Create field tagged tuple.
-spec make_field(atom(), string(), list()) -> field_data().
make_field(dynamicfield, FieldName, Options)  ->
    {dynamicfield, [{name, FieldName}|merge(Options, ?FIELD_DEFAULTS)]};
make_field(field, FieldName, Options) ->
    {field, [{name, FieldName}|merge(Options, ?FIELD_DEFAULTS)]}.

%% @doc Update schema with change(s).
-spec update_schema(add | remove, string(), field_data() | string()) ->
                           ok | schema_err().
update_schema(add, Name, Field) ->
    {Name, Field};
update_schema(remove, Name, FieldName) ->
    {Name, FieldName}.

%% TODO: Use Riak-Cli or place *parse_options* in a one place for all consoles
-spec parse_options(list(string())) -> list({string(), string()}).
parse_options(Options) ->
    parse_options(Options, []).

parse_options([], Acc) ->
    Acc;
parse_options([H|T], Acc) ->
    case re:split(H, "=", [{parts, 2}, {return, list}]) of
        [Key, Value] when is_list(Key), is_list(Value) ->
            parse_options(T, [{string:to_lower(Key), Value}|Acc]);
        _Other ->
            throw({error, {invalid_option, H}})
    end.

%% @doc Reads and returns `RawSchema` from file path.
-spec read_schema(string()) -> {ok, raw_schema()} | schema_err().
read_schema(Path) ->
    AbsPath = filename:absname(Path),
    case file:read_file(AbsPath) of
        {ok, RawSchema} ->
            {ok, RawSchema};
        {error, enoent} ->
            io:format("No such file or directory: ~s~n", [Path]),
            throw({fileReadError, enoent});
        {error, Reason} ->
            ?ERROR("Error reading file ~s:~p", [Path, Reason]),
            io:format("Error reading file ~s, see log for details~n", [Path]),
            throw({fileReadError, Reason})
    end.

%% @doc Merge field defaults.
-spec merge([{atom(), any()}], [{atom(), any()}]) -> [{atom(), any()}].
merge(Overriding, Other) ->
    lists:ukeymerge(1, lists:ukeysort(1, Overriding),
                    lists:ukeysort(1, Other)).

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

read_schema_test() ->
    {ok, CurrentDir} = file:get_cwd(),
    MissingSchema = CurrentDir ++ "/foo.xml",
    GoodSchema = filename:join([?YZ_PRIV, "default_schema.xml"]),
    GoodOutput = read_schema(GoodSchema),
    ?assertMatch({ok, _}, GoodOutput),
    {ok, RawSchema} = GoodOutput,
    ?assert(is_binary(RawSchema)),
    ?assertThrow({fileReadError, enoent}, read_schema(MissingSchema)).

parse_options_test() ->
    EmptyOptions = [],
    GoodOptions = ["foo=bar", "bar=baz", "foobar=barbaz"],
    BadOptions = ["hey", "foo"],
    ?assertEqual([], parse_options(EmptyOptions)),
    %% first errored option will be thrown
    ?assertThrow({error, {invalid_option, "hey"}}, parse_options(BadOptions)),
    ?assertEqual([{"foobar", "barbaz"}, {"bar", "baz"}, {"foo", "bar"}],
                 parse_options(GoodOptions)).

merge_test() ->
    Overriding = [{type, "integer"}, {stored, "true"}, {indexed, "true"}],
    Other = ?FIELD_DEFAULTS,
    Merged = merge(Overriding, Other),
    Expected = [{indexed, "true"}, {multiValued, "true"}, {stored, "true"},
                {type, "integer"}],
    ?assertEqual(Expected, Merged).

make_field_test() ->
    Field = make_field(field, "person",
                             parse_options(["foo=bar", "bar=baz"])),
    DynamicField = make_field(dynamicfield, "person", []),
    ?assertMatch({field, [_|_]}, Field),
    ?assertMatch({dynamicfield, [_|_]}, DynamicField),
    {_, FieldItems} = Field,
    {_, DynamicFieldItems} = DynamicField,
    %% + 2 new options + 1 for the field name
    ?assertEqual(length(FieldItems), length(?FIELD_DEFAULTS) + 3),
    %% + 1 for the field name
    ?assertEqual(length(DynamicFieldItems), length(?FIELD_DEFAULTS) + 1).

-endif.
