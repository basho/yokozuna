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

-module(yokozuna).
-include("yokozuna.hrl").
-compile(export_all).


%%%===================================================================
%%% API
%%%===================================================================

%% @doc Index the given object `O'.
-spec index(string(), riak_object:riak_object()) -> ok | {error, term()}.
index(Index, O) ->
    yz_solr:index(Index, [yz_doc:make_doc(O, <<"FPN">>, <<"Partition">>)]).

%% @doc Return the set of unique logical partitions stored on this
%%      node for the given `Index'.
-spec partition_list(string()) -> ordset(lp()).
partition_list(Index) ->
    Resp = yz_solr:partition_list(Index),
    Struct = mochijson2:decode(Resp),
    Path = [<<"facet_counts">>, <<"facet_fields">>, ?YZ_PN_FIELD_B],
    Facets = yz_solr:get_path(Struct, Path),
    %% Facets is a list of field values followed by their
    %% corresponding count.  The `is_binary' filter is done to remove
    %% the counts and leave only the partitions.
    Partitions = lists:filter(fun erlang:is_binary/1, Facets),
    ordsets:from_list([?BIN_TO_INT(P) || P <- Partitions]).

search(Index, Query, Mapping) ->
    yz_solr:search(Index, [{q, Query}], Mapping).

solr_port(Node, Ports) ->
    proplists:get_value(Node, Ports).
