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
    {_Good, Down} = riak_core_util:rpc_every_member_ann(yokozuna, switch_to_yokozuna, [], 5000),
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
-spec create_schema([string()|string()]) -> ok | schema_err().
create_schema([Name, Path]) ->
    try
        RawSchema = read_schema(Path),
        FMTName = list_to_atom(Name),
        case yz_schema:store(list_to_binary(Name), RawSchema) of
            ok ->
                io:format("~p schema created~n", [FMTName]),
                ok;
            {error, _} ->
                io:format("Error creating schema ~p", [FMTName]),
                error
        end
    catch fileReadError:exitError ->
           exitError
    end.

%% @doc Shows solr schema for name passed in.
-spec show_schema([string()]) -> ok | schema_err().
show_schema([Name]) ->
    FMTName = list_to_atom(Name),
    case yz_schema:get(list_to_binary(Name)) of
        {ok, R} ->
            io:format("Schema ~p:~n~s", [FMTName, binary_to_list(R)]),
            ok;
        {error, notfound} ->
            io:format("Schema ~p doesn't exist~n", [FMTName]),
            error
    end.

%% @doc
-spec add_to_schema([string()|string()]) -> ok | schema_err().
add_to_schema([Name|Opts]) ->
    {Name, Opts}.

%% @doc
-spec remove_from_schema([string()|string()]) -> ok | schema_err().
remove_from_schema([Name, FieldName]) ->
    {Name, FieldName}.

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Reads and returns `RawSchema` from file path.
-spec read_schema(string()) -> raw_schema() | schema_err().
read_schema(Path) ->
    AbsPath = filename:absname(Path),
    case file:read_file(AbsPath) of
        {ok, RawSchema} ->
            RawSchema;
        {error, enoent} ->
            io:format("No such file or directory: ~s~n", [Path]),
            throw({fileReadError, enoent});
        {error, Reason} ->
            ?ERROR("Error reading file ~s:~p", [Path, Reason]),
            io:format("Error reading file ~s, see log for details~n", [Path]),
            throw({fileReadError, Reason})
    end.
