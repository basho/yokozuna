-module(yz_dt_test).
-compile(export_all).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CFG, [{riak_core, [{ring_creation_size, 16}]},
              {yokozuna, [{enabled, true}]}]).
-define(COUNTER, <<"counters">>).
-define(SET, <<"sets">>).
-define(MAP, <<"maps">>).
-define(TYPES,
        [{?COUNTER, counter},
         {?SET, set},
         {?MAP, map}]).

-import(yz_rt, [
                connection_info/1,
                create_index/2,
                search_expect/5,
                wait_for_index/2
               ]).

-define(assertSearch(Solr, Index, Field, Query, Count),
        ?assertEqual(ok, rt:wait_until(fun() -> search_expect(Solr, Index, Field, Query, Count) end))).


confirm() ->
    application:start(ibrowse),
    %% Build a cluster
    [Node|_] = Nodes = rt:build_cluster(4, ?CFG),
    [{Node, CI}|_] = connection_info(Nodes),
    PB = rt:pbc(Node),
    HTTP = proplists:get_value(http, CI),
    [ begin
          %% Create an index for each type (default schema)
          create_index(Node, BType),
          wait_for_index(Nodes, BType),
          %% Create bucket types for datatypes with given indexes
          rt:create_and_activate_bucket_type(Node, BType, [{datatype, Type},
                                                           {allow_mult, true},
                                                           {search_index, BType}])
      end || {BType, Type} <- ?TYPES ],
    %% Update some datatypes
    counter_update(PB),
    set_update(PB),
    map_update(PB),
    %% Search the index for the types
    counter_search(HTTP),
    set_search(HTTP),
    map_search(HTTP),
    pass.

counter_update(PB) ->
    ?assertEqual(ok, riakc_pb_socket:update_type(PB, {?COUNTER, <<"1">>}, <<"10">>, {counter, {increment, 10}, undefined})),
    ?assertEqual(ok, riakc_pb_socket:update_type(PB, {?COUNTER, <<"2">>}, <<"100">>, {counter, {increment, 100}, undefined})),
    ?assertEqual(ok, riakc_pb_socket:update_type(PB, {?COUNTER, <<"1">>}, <<"1000">>, {counter, {decrement, 1000}, undefined})).

counter_search(Solr) ->
    ?assertSearch(Solr, ?COUNTER, "counter", "10", 1),
    ?assertSearch(Solr, ?COUNTER, "counter", "[0 TO 999]", 2),
    ?assertSearch(Solr, ?COUNTER, "counter", "[99 TO 999]", 1).

set_update(PB) ->
    Dynamos = lists:foldl(fun riakc_set:add_element/2, riakc_set:new(),
                          [<<"Riak">>, <<"Cassandra">>, <<"Voldemort">>, <<"Couchbase">>]),
    Erlangs = lists:foldl(fun riakc_set:add_element/2, riakc_set:new(),
                          [<<"Riak">>, <<"Couchbase">>, <<"CouchDB">>]),
    ?assertEqual(ok, riakc_pb_socket:update_type(PB, {?SET, <<"databass">>}, <<"dynamo">>, riakc_set:to_op(Dynamos))),
    ?assertEqual(ok, riakc_pb_socket:update_type(PB, {?SET, <<"databass">>}, <<"erlang">>, riakc_set:to_op(Erlangs))).

set_search(Solr) ->
    ?assertSearch(Solr, ?SET, "set", "Riak", 2),
    ?assertSearch(Solr, ?SET, "set", "CouchDB", 1),
    ?assertSearch(Solr, ?SET, "set", "Voldemort", 1),
    ?assertSearch(Solr, ?SET, "set", "C*", 2).

map_update(PB) ->
    Sam = lists:foldl(fun({Key, Fun}, Map) ->
                              riakc_map:update(Key, Fun, Map)
                      end, riakc_map:new(),
                      [{{<<"friends">>, set},
                        fun(Set) ->
                                riakc_set:add_element(<<"Sean">>,
                                                      riakc_set:add_element(<<"Russell">>, Set))
                        end},
                       {{<<"name">>, register},
                        fun(Reg) ->
                                riakc_register:set(<<"Sam Elliott">>, Reg)
                        end},
                       {{<<"student">>, flag}, fun riakc_flag:enable/1},
                       {{<<"burgers">>, counter}, fun(C) -> riakc_counter:increment(10, C) end}]),

    Russell = lists:foldl(fun({Key, Fun}, Map) ->
                                  riakc_map:update(Key, Fun, Map)
                          end, riakc_map:new(),
                          [{{<<"friends">>, set},
                            fun(Set) ->
                                    riakc_set:add_element(<<"Sean">>,
                                                          riakc_set:add_element(<<"Sam">>, Set))
                            end},
                           {{<<"name">>, register},
                            fun(Reg) ->
                                    riakc_register:set(<<"Russell Brown">>, Reg)
                            end},
                           {{<<"student">>, flag}, fun riakc_flag:disable/1},
                           {{<<"burgers">>, counter}, fun(C) -> riakc_counter:increment(100, C) end}]),

    Sean = lists:foldl(fun({Key, Fun}, Map) ->
                               riakc_map:update(Key, Fun, Map)
                       end, riakc_map:new(),
                       [{{<<"friends">>, set},
                         fun(Set) ->
                                 riakc_set:add_element(<<"Russell">>,
                                                       riakc_set:add_element(<<"Joe">>, Set))
                         end},
                        {{<<"name">>, register},
                         fun(Reg) ->
                                 riakc_register:set(<<"Sean Cribbs">>, Reg)
                         end},
                        {{<<"office">>, map},
                         fun(M) ->
                                 riakc_map:update(
                                   {<<"cats">>, counter},
                                   fun(C) -> riakc_counter:increment(2, C) end,
                                   riakc_map:update(
                                     {<<"location">>, register},
                                     fun(R) -> riakc_register:set(<<"The Cornfields, IL USA">>, R) end, M))
                         end}]),
    ?assertEqual(ok, riakc_pb_socket:update_type(PB, {?MAP, <<"people">>},<<"lenary">> ,riakc_map:to_op(Sam))),
    ?assertEqual(ok, riakc_pb_socket:update_type(PB, {?MAP, <<"people">>},<<"rdb">> ,riakc_map:to_op(Russell))),
    ?assertEqual(ok, riakc_pb_socket:update_type(PB, {?MAP, <<"people">>},<<"scribbs">> ,riakc_map:to_op(Sean))).

map_search(Solr) ->
    ?assertSearch(Solr, ?MAP, "burgers_counter", "*", 2),
    ?assertSearch(Solr, ?MAP, "burgers_counter", "[11 TO 1000]", 1),
    ?assertSearch(Solr, ?MAP, "student_flag", "*", 2),
    ?assertSearch(Solr, ?MAP, "student_flag", "true", 1),
    ?assertSearch(Solr, ?MAP, "name_register", "S*", 2),
    ?assertSearch(Solr, ?MAP, "office_map.cats_counter", "1", 0),
    ?assertSearch(Solr, ?MAP, "office_map.cats_counter", "2", 1),
    ?assertSearch(Solr, ?MAP, "office_map.location_register", "*", 1),
    ?assertSearch(Solr, ?MAP, "friends_set", "Sam", 1),
    ?assertSearch(Solr, ?MAP, "friends_set", "Joe", 1),
    ?assertSearch(Solr, ?MAP, "friends_set", "Russell", 2).
