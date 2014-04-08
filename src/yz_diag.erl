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
%%
%% @doc This module provides visibility into the state of Yokozuna. It
%% is meant to aid in diagnosing Yokozuna when problems arise.
%%
%% All API functions should return Erlang data structures. They SHOULD
%% NOT make logging calls or write to stdout/stderr. There may be
%% functions which do that but they should be separate; built on top
%% of these functions. It is very important to return Erlang data
%% structures which are properly documented and spec'd so that they
%% can be used by different consumers.
-module(yz_diag).
-compile(export_all).
-include("yokozuna.hrl").
-type either(Term) :: Term | {error, Reason :: term()}.

%%%===================================================================
%%% Types of Diagnostic Information
%%%===================================================================

-type component_status() :: [{component(), boolean()}].

-type error_doc_count() :: {index_name(), either(non_neg_integer())}.
-type error_doc_counts() :: [error_doc_count()].

-type searcher_stat() :: {caching, boolean()} |
                         {deleted_docs, non_neg_integer()} |
                         {index_version, non_neg_integer()} |
                         {max_doc, non_neg_integer()} |
                         {num_docs, non_neg_integer()} |
                         {opened, iso8601()} |
                         {warmup_time, non_neg_integer()}.
-type searcher_stats() :: [searcher_stat()].

-type service_avail() :: [node()].

%% NOTE: If you are adding a type please keep the list in alphabetical
%% order.
%%
%% `error_doc_counts' - The number of error documents per index.
%%
%% `indexes_in_meta' - List of indexes according to Yokozuna metadata.
%%
%% `service_avail' - List of nodes where the Yokozuna service is available.
%%
%% `{node(), [local_datum()]}' - Data for all nodes running the
%%   Yokozuna service.
-type diag_datum() :: {error_doc_counts, error_doc_counts()} |
                      {indexes_in_meta, indexes()} |
                      {service_avail, service_avail()} |
                      {node(), [local_datum()]}.

%% Local data is per-node.
%%
%% `component_status' - The status of all Yokozuna components.  `true'
%%   means enabled, `false' means disabled.
%%
%% `indexes_in_solr' - List of indexes according to Solr.
%%
%% `searcher_stats' - Various Lucene Searcher statistics for each index.
-type local_datum() :: {component_status, component_status()} |
                       {indexes_in_solr, [index_name()]} |
                       {searcher_stats, searcher_stats()}.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return a list containing various diagnostic information that
%% might be useful.
-spec all() -> [diag_datum()].
all() ->
    IndexesInMeta = yz_index:get_indexes_from_meta(),
    ErroDocCounts = error_doc_counts(IndexesInMeta),
    NodesAvail = riak_core_node_watcher:nodes(?YZ_SVC_NAME),
    NodeDiags = [{N, rpc:call(N, ?MODULE, all_local, [])} || N <- NodesAvail],
    [
     {error_doc_counts, ErroDocCounts},
     {indexes_in_meta, IndexesInMeta},
     {service_avail, NodesAvail}|NodeDiags
    ].

%% @doc Return a list of diagnostic information for the local node.
-spec all_local() -> [local_datum()].
all_local() ->
    Indexes = indexes(),
    SearcherStats = searcher_stats(Indexes),
    [
     {component_status, component_status()},
     {indexes_in_solr, Indexes},
     {searcher_stats, SearcherStats}
    ].

%% @doc Return the status of all Yokozuna components.  `true' means
%% enabled, `false' means disabled.
-spec component_status() -> component_status().
component_status() ->
    [
     {index, yokozuna:is_enabled(index)},
     {search, yokozuna:is_enabled(search)}
    ].

%% @doc Return the list of indexes.
-spec indexes() -> either([index_name()]).
indexes() ->
    case yz_solr:cores() of
        {ok, Indexes} ->
            Indexes;
        {error, Reason} ->
            {error, {"failed to get list of indexes from Solr", Reason}}
    end.

%% @doc Return various Lucene Searcher stats.  Either a single `Index'
%% or list of `Indexes' can be passed.
-spec searcher_stats(index_name() | [index_name()]) ->
                            either(searcher_stats() | [{node(), searcher_stats()}]).
searcher_stats(Indexes) when is_list(Indexes) ->
    [{Index, searcher_stats(Index)} || Index <- Indexes];
searcher_stats(Index) ->
    case yz_solr:mbeans_and_stats(Index) of
        {ok, JSON} ->
            MBeans = kvc:path([<<"solr-mbeans">>], mochijson2:decode(JSON)),
            CoreObj = lists:nth(2, MBeans),
            Stats = kvc:path([<<"searcher">>, <<"stats">>], CoreObj),
            Caching = kvc:path([<<"caching">>], Stats),
            DeletedDocs = kvc:path([<<"deletedDocs">>], Stats),
            IndexVersion = kvc:path([<<"indexVersion">>], Stats),
            MaxDoc = kvc:path([<<"maxDoc">>], Stats),
            NumDocs = kvc:path([<<"numDocs">>], Stats),
            Opened = kvc:path([<<"openedAt">>], Stats),
            WarmupTime = kvc:path([<<"warmupTime">>], Stats),
            [
             {caching, Caching},
             {deleted_docs, DeletedDocs},
             {index_version, IndexVersion},
             {max_doc, MaxDoc},
             {num_docs, NumDocs},
             {opened, Opened},
             {warmup_time, WarmupTime}
            ];
        {error, Reason} ->
            {error, {"failed to get search stats from Solr", Reason}}
    end.

%% @doc Return the `Count' of error documents for each index in
%% `Indexes'.  Error documents represent objects which could not be
%% properly extracted.  This typically happens when the value is a
%% malformed version of the content-type.  E.g. JSON without a closing
%% brace.
-spec error_doc_counts([index_name()]) -> error_doc_counts().
error_doc_counts(Indexes) ->
    [error_doc_counts_2(Index) || Index <- Indexes].

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
-spec error_doc_counts_2(index_name()) -> error_doc_count().
error_doc_counts_2(Index) ->
    Params = [{q, <<?YZ_ERR_FIELD_S,":1">>},
              {fl, ?YZ_ID_FIELD_B},
              {wt, <<"json">>}],
    case yz_solr:dist_search(Index, Params) of
        {ok, {_, Body}} ->
            Struct = mochijson2:decode(Body),
            Count = kvc:path([<<"response">>, <<"numFound">>], Struct),
            {Index, Count};
        {error, _} = Err ->
            {Index, Err}
    end.
