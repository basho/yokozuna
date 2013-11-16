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

%% @doc Disable the given `Component'.  The main reason for disabling
%%      a component is to help in diagnosing issues in a live,
%%      production environment.  E.g. the `search' component may be
%%      disabled to take search load off the cluster while
%%      investigating latency issues.
-spec disable(component()) -> ok.
disable(Component) ->
    application:set_env(?YZ_APP_NAME, {component, Component}, false).

%% @doc Enabled the given `Component'.  This only needs to be called
%%      if a component was previously disabled as all components are
%%      considered enabled by default.
-spec enable(component()) -> ok.
enable(Component) ->
    application:set_env(?YZ_APP_NAME, {component, Component}, true).

%% @doc Determine if the given `Component' is enabled or not.  If a
%%      component is not explicitly disabled then it is considered
%%      enabled.
-spec is_enabled(component()) -> boolean().
is_enabled(Component) ->
    app_helper:get_env(?YZ_APP_NAME, {component, Component}, true).

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
-spec mapred_search(pipe(), [binary()], pos_integer()) -> ok.
mapred_search(Pipe, [Index, Query], _Timeout) ->
    mapred_search(Pipe, [Index, Query, <<"">>], _Timeout);
mapred_search(Pipe, [Index, Query, Filter], _Timeout) ->
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
    ok = search_fold(Index, Query, Filter, F, ok),
    riak_pipe:eoi(Pipe).

%% @doc Return the ordered set of unique logical partitions stored on
%%      the local node for the given `Index'.
-spec partition_list(index_name()) -> ordset(lp()).
partition_list(Index) ->
    {ok, Resp} = yz_solr:partition_list(Index),
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
              {fl, <<?YZ_RT_FIELD_S,",",?YZ_RB_FIELD_S,",",?YZ_RK_FIELD_S>>},
              {omitHeader, <<"true">>},
              {wt, <<"json">>}],
    {_, Body} = yz_solr:dist_search(Index, Params),
    case extract_docs(Body) of
        [] ->
            Acc;
        Docs ->
            Positions = positions(hd(Docs)),
            E = extract_results(Docs, Positions),
            search_fold(E, Start, Params, Positions, Index, Query, Filter, F, Acc)
    end.

search(Index, Query) ->
    yz_solr:dist_search(Index, [{q, Query}]).

solr_port(Node, Ports) ->
    proplists:get_value(Node, Ports).

%% @doc Switch handling of HTTP and PB search requests to Yokozuna.
%% This is used during migration from Riak Search.
%%
%% NOTE: Yokozuna is added to the route first to avoid period where no
%% resource is registered for the search route.  Since a new route is
%% added to head of list Yokozuna will be picked over Riak Search
%% while both routes are in the list.
-spec switch_to_yokozuna() -> ok.
switch_to_yokozuna() ->
    ok = riak_api_pb_service:swap(yz_pb_search, 27, 28),
    ok = webmachine_router:add_route({["solr", index, "select"], yz_wm_search, []}),
    ok = webmachine_router:remove_resource(riak_solr_indexer_wm),
    ok = webmachine_router:remove_resource(riak_solr_searcher_wm),
    ok.

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
%%
%% @doc Decode the `Body' and extract the doc list.
-spec extract_docs(binary()) -> Docs :: term().
extract_docs(Body) ->
    Obj = mochijson2:decode(Body),
    kvc:path([<<"response">>, <<"docs">>], Obj).

%% @private
%%
%% @doc Extract the bucket/key results from the `Body' of a search
%%      result.
-type positions() :: {integer(), integer(), integer()}.
-spec extract_results(term(), positions()) -> [{bucket(),binary(),[term()]}].
extract_results(Docs, {TP, BP, KP}) ->
    [begin
         {_, BType} = lists:nth(TP, Fields),
         {_, BName} = lists:nth(BP, Fields),
         {_, Key} = lists:nth(KP, Fields),
         {{BType, BName}, Key, []}
     end|| {struct, Fields} <- Docs].

%% @private
%%
%% @doc Determine the positions of the bucket-type, bucket-name and
%% key in the doc list.
%%
%% N.B. This ugly hack is required because as far as I can tell
%% there is not defined order of the fields inside the results
%% returned by Solr.
-spec positions({struct, [tuple()]}) ->
                       {integer(), integer(), integer()}.
positions({struct,[X, Y, Z]}) ->
    L1 = [{element(1, X), 1}, {element(1, Y), 2}, {element(1, Z), 3}],
    {value, {_,BTypePos}, L2} = lists:keytake(?YZ_RT_FIELD_B, 1, L1),
    {value, {_,BNamePos}, L3} = lists:keytake(?YZ_RB_FIELD_B, 1, L2),
    {value, {_,KeyPos}, _} = lists:keytake(?YZ_RK_FIELD_B, 1, L3),
    {BTypePos, BNamePos, KeyPos}.

%% @private
%%
%% @doc This is the interal part of `search_fold' where the actual
%%      iteration happens.
-spec search_fold(list(), non_neg_integer(), list(), positions(), index_name(),
                  binary(), binary(), fold_fun(), Acc::term()) ->
                         Acc::term().
search_fold([], _, _, _, _, _, _, _, Acc) ->
    Acc;
search_fold(Results, Start, Params, Positions, Index, Query, Filter, F, Acc) ->
    F(Results, Acc),
    Start2 = Start + 10,
    Params2 = lists:keystore(start, 1, Params, {start, Start2}),
    {_, Body} = yz_solr:dist_search(Index, Params2),
    Docs = extract_docs(Body),
    E = extract_results(Docs, Positions),
    search_fold(E, Start2, Params, Positions, Index, Query, Filter, F, Acc).
