%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%--------------------------------------------------------------------
-module(yz_faceted_search).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include("yokozuna.hrl").

-define(INDEX, <<"restaurants_index">>).
-define(TYPE, <<"restaurants">>).
-define(BUCKET, {?TYPE, <<"goodburger">>}).
-define(SCHEMANAME, <<"restaurants_schema">>).

-define(FACETED_SCHEMA,
<<"<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_err\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>

   <field name=\"name\" type=\"string\" indexed=\"true\" stored=\"true\"/>
   <field name=\"city\" type=\"string\" indexed=\"true\" stored=\"true\"/>
   <field name=\"state\" type=\"string\" indexed=\"true\" stored=\"true\"/>
   <field name=\"price\" type=\"integer\" indexed=\"true\" stored=\"true\"/>
</fields>
<uniqueKey>_yz_id</uniqueKey>
<types>
   <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />

   <fieldType name=\"string\" class=\"solr.StrField\" sortMissingLast=\"true\" />
   <fieldType name=\"integer\" class=\"solr.TrieIntField\" />
</types>
</schema>">>).

-define(CONFIG,
        [{riak_core,
          [{ring_creation_size, 8}]},
         {yokozuna,
          [{enabled, true}]}
        ]).

confirm() ->
    Cluster = rt:build_cluster(4, ?CONFIG),
    rt:wait_for_cluster_service(Cluster, yokozuna),

    ok = yz_rt:create_indexed_bucket_type(Cluster, ?TYPE, ?INDEX,
                                          ?SCHEMANAME, ?FACETED_SCHEMA),

    put_restaurants(Cluster, ?BUCKET),
    verify_field_faceting(Cluster, ?INDEX),
    verify_query_faceting(Cluster, ?INDEX),
    pass.

-define(RESTAURANTS,
        [
         {<<"Senate">>, <<"Cincinnati">>, <<"Ohio">>, 21},
         {<<"Boca">>, <<"Cincinnati">>, <<"Ohio">>, 74},
         {<<"Terry's Turf Club">>, <<"Cincinnati">>, <<"Ohio">>, 16},
         {<<"Thurman Cafe">>, <<"Columbus">>, <<"Ohio">>, 9},
         {<<"Otto's">>, <<"Covington">>, <<"Kentucky">>, 55}
        ]).

put_restaurants(Cluster, Bucket) ->
    Restaurants = [create_restaurant_json(Name, City, State, Price) ||
                   {Name, City, State, Price} <- ?RESTAURANTS],
    Keys = yz_rt:gen_keys(length(Restaurants)),
    Pid = rt:pbc(hd(Cluster)),
    lists:foreach(fun({Key, Restaurant}) ->
                          put_restaurant(Pid, Bucket, Key, Restaurant)
                  end,
                  lists:zip(Keys, Restaurants)),
    yz_rt:commit(Cluster, ?INDEX),
    % wait until the expected number of documents are queryable
    yz_rt:search_expect(Cluster, ?INDEX, "name", "*", length(?RESTAURANTS)).

-spec create_restaurant_json(binary(), binary(), binary(), integer()) -> binary().
create_restaurant_json(Name, City, State, Price) ->
    PriceBinary = list_to_binary(integer_to_list(Price)),
    <<"{\"name\":\"", Name/binary, "\",",
      "\"city\":\"", City/binary, "\",",
      "\"state\":\"", State/binary, "\",",
      "\"price\":\"", PriceBinary/binary, "\"}">>.

put_restaurant(Pid, Bucket, Key, Restaurant) ->
    Obj = riakc_obj:new(Bucket, Key, Restaurant, "application/json"),
    riakc_pb_socket:put(Pid, Obj).

verify_field_faceting(Cluster, Index) ->
    HP = yz_rt:host_port(Cluster),
    Params = [{facet, true}, {'facet.field', state}, {'facet.mincount', 3}],
    lager:info("Field faceting: ~p, ~p, ~p", [HP, Index, Params]),
    {ok, "200", _Hdr, Res} = yz_rt:search(HP, Index, "name", "*", Params),
    Struct = mochijson2:decode(Res),
    lager:debug("Field faceting results: ~p", [Struct]),

    NumFound = kvc:path([<<"response">>, <<"numFound">>], Struct),
    ?assertEqual(5, NumFound),

    StateCounts = kvc:path([<<"facet_counts">>,
                            <<"facet_fields">>,
                            <<"state">>],
                           Struct),
    %% We expect to see all 4 Ohio restaurants in the search results, but none
    %% of the Kentucky restaurants because there is only one in the test data
    %% set, and facet.mincount is set to 3.
    ?assertEqual([<<"Ohio">>,4], StateCounts).

-define(PRICE_RANGE_1, <<"price:[1 TO 29]">>).
-define(PRICE_RANGE_2, <<"price:[30 TO 100]">>).
verify_query_faceting(Cluster, Index) ->
    HP = yz_rt:host_port(Cluster),
    Params = [{facet, true},
              {'facet.mincount', 1},
              {'facet.query', "price:[1 TO 29]"},
              {'facet.query', "price:[30 TO 100]"}],
    %% Verify 10 times because of non-determinism in coverage
    [verify_query_facets(HP, Index, Params) || _ <- lists:seq(1, 10)],
    ok.

verify_query_facets(HP, Index, Params) ->
    lager:info("Query faceting: ~p, ~p, ~p", [HP, Index, Params]),
    {ok, "200", _Hdr, Res} = yz_rt:search(HP, Index, "state", "Ohio", Params),
    Struct = mochijson2:decode(Res),
    lager:debug("Query faceting results: ~p", [Struct]),
    {struct, FacetQueries} = kvc:path([<<"facet_counts">>,
                                       <<"facet_queries">>],
                                      Struct),
    ?assertMatch({_Key, 3}, lists:keyfind(?PRICE_RANGE_1, 1, FacetQueries)),
    ?assertMatch({_Key, 1}, lists:keyfind(?PRICE_RANGE_2, 1, FacetQueries)),
    ok.
