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

-type fold_fun() :: fun(([term()], Acc::term()) -> Acc::term()).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Use the results of a search as input to a map-reduce job.
%%
%% `Pipe' - Reference to the Riak Pipe job responsible for running the
%%   map/reduce job.
%%
%% `Index' - The name of the index to run the search against.
%%
%% `Query' - The query to run.
%%
%% `Filter' - The filter query to run.  This is passed as a filter
%%   query (fq) to Solr meaning it doesn't affect scoring and may be
%%   cached by Solr if caching is enabled.
%%
%% `Timeout' - This parameter is ignored.  It is only here for
%%   backwards compatibility.
%%
%% TODO: The `Timeout' parameter is meant to act as a timeout for the
%%   entire job.  However, this timeout should be tracked by the
%%   process making the map/reduce request.  Accoding to Bryan Fink
%%   both PB and HTTP already keep track of the timeout and teardown
%%   the job if the limit is reached.  Rather than keep track of this
%%   timeout in multiple places, potentially causing weird runtime
%%   issues when the timeout is reached, Yokozuna chooses to ignore it
%%   for now and assume an upstream process is dealing with it.  This
%%   timeout value should be revisited in the future and see if it
%%   can't be removed from the API completely.
-spec mapred_search(pipe(), [term()], pos_integer()) -> ok.
mapred_search(Pipe, [Index, Query], _Timeout) ->
    mapred_search(Pipe, [Index, Query, <<"">>], _Timeout);
mapred_search(Pipe, [Index, Query, Filter], _Timeout) ->
    Index2 = case is_binary(Index) of
                 false -> Index;
                 true -> binary_to_list(Index)
             end,
    %% Function to convert `search_fold' results into pipe format.
    Q = fun({Bucket,Key,Props}) ->
                riak_pipe:queue_work(Pipe, {{Bucket, Key}, {struct, Props}})
        end,
    F = fun(Results, Acc) ->
                %% TODO: is there a way to send a batch of results?
                %% Avoiding the `foreach` would be nice.
                lists:foreach(Q, Results),
                Acc
        end,
    ok = search_fold(Index2, Query, Filter, F, ok),
    riak_pipe:eoi(Pipe).

%% @doc Return the set of unique logical partitions stored on this
%%      node for the given `Index'.
-spec partition_list(string()) -> ordset(lp()).
partition_list(Index) ->
    Resp = yz_solr:partition_list(Index),
    Struct = mochijson2:decode(Resp),
    Path = [<<"facet_counts">>, <<"facet_fields">>, ?YZ_PN_FIELD_B],
    Facets = kvc:path(Path, Struct),
    %% Facets is a list of field values followed by their
    %% corresponding count.  The `is_binary' filter is done to remove
    %% the counts and leave only the partitions.
    Partitions = lists:filter(fun erlang:is_binary/1, Facets),
    ordsets:from_list([?BIN_TO_INT(P) || P <- Partitions]).

%% @doc Executing a search folding over the results executing the
%%      function `F' for each result.
%%
%% `Index' - The index to search.
%%
%% `Query' - The query to run.
%%
%% `Filter' - The filter to use.  This will be passed as a filter
%%   query (fq) to Solr meaning it doesn't affect scoring and the
%%   result may be cached for future use.
%%
%% `F' - The fold function which must be a two-arity.  The first
%%   argument is a list of results.  The second is the accumulator
%%   `Acc'.
%%
%% `Acc' - The initial value of the accumulator.  It may be any
%%   arbitrary Erlang term.
-spec search_fold(index_name(), binary(), binary(), fold_fun(), T::term()) ->
                         T::term().
search_fold(Index, Query, Filter, F, Acc) ->
    Start = 0,
    Params = [{q, Query},
              {fq, Filter},
              {start, Start},
              {rows, 10},
              {fl, <<?YZ_RB_FIELD_S,",",?YZ_RK_FIELD_S>>},
              {omitHeader, <<"true">>},
              {wt, <<"json">>}],
    Mapping = yz_events:get_mapping(),
    {_, Body} = yz_solr:dist_search(Index, Params, Mapping),
    E = extract_results(Body),
    search_fold(E, Start, Params, Mapping, Index, Query, Filter, F, Acc).

search(Index, Query, Mapping) ->
    yz_solr:dist_search(Index, [{q, Query}], Mapping).

solr_port(Node, Ports) ->
    proplists:get_value(Node, Ports).

%% @doc get an associated flag, true if some action
%%      (eg indexing, searching) should be supressed
-spec noop_flag(index|search) -> boolean().
noop_flag(index) ->
    app_helper:get_env(?YZ_APP_NAME, index_noop, false);
noop_flag(search) ->
    app_helper:get_env(?YZ_APP_NAME, search_noop, false).

%% @doc set an associated flag, true if some action
%%      (eg indexing, searching) should be supressed
-spec noop_flag(index|search, boolean()) -> ok.
noop_flag(index, Switch) ->
    ?INFO("Indexing Objects in yokozuna has been changed to ~s", [Switch]),
    application:set_env(?YZ_APP_NAME, index_noop, Switch);
noop_flag(search, Switch) ->
    ?INFO("Ability to search yokozuna has been changed to ~s", [Switch]),
    application:set_env(?YZ_APP_NAME, search_noop, Switch).

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
%%
%% @doc Extract the bucket/key results from the `Body' of a search
%%      result.
-spec extract_results(binary()) -> [{binary(),binary(),[term()]}].
extract_results(Body) ->
    Obj = mochijson2:decode(Body),
    Docs = kvc:path([<<"response">>, <<"docs">>], Obj),
    %% N.B. This ugly hack is required because as far as I can tell
    %% there is not defined order of the fields inside the results
    %% returned by Solr.
    [begin
         case {X,Y} of
             {{?YZ_RB_FIELD_B,B}, {?YZ_RK_FIELD_B,K}} ->
                 {B,K,[]};
             {{_,K},{_,B}} ->
                 {B,K,[]}
         end
     end|| {struct,[X,Y]} <- Docs].

%% @private
%%
%% @doc This is the interal part of `search_fold' where the actual
%%      iteration happens.
-spec search_fold(list(), non_neg_integer(), list(), list(), index_name(),
                  binary(), binary(), fold_fun(), Acc::term()) ->
                         Acc::term().
search_fold([], _, _, _, _, _, _, _, Acc) ->
    Acc;
search_fold(Results, Start, Params, Mapping, Index, Query, Filter, F, Acc) ->
    F(Results, Acc),
    Start2 = Start + 10,
    Params2 = lists:keystore(start, 1, Params, {start, Start2}),
    {_, Body} = yz_solr:dist_search(Index, Params2, Mapping),
    E = extract_results(Body),
    search_fold(E, Start2, Params, Mapping, Index, Query, Filter, F, Acc).

