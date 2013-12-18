-module(yz_dt_extractor_tests).
-compile(export_all).
-include_lib("yz_test.hrl").
-include_lib("riak_kv/include/riak_kv_types.hrl").


%% Test counter extract
counter_test() ->
    CounterBin = binary_crdt(counter),
    Result = yz_dt_extractor:extract(CounterBin),
    Expect = [{<<"counter">>, <<"10">>}],

    valid_extraction(Result, Expect).

%% Test set extract
set_test() ->
    SetBin = binary_crdt(set),
    Result = yz_dt_extractor:extract(SetBin),
    Expect = [{<<"set">>, <<"Riak">>},
              {<<"set">>, <<"Cassandra">>},
              {<<"set">>, <<"Voldemort">>}],

    valid_extraction(Result, Expect).

%% Test map extract
map_test() ->
    MapBin = binary_crdt(map),
    Result = yz_dt_extractor:extract(MapBin),
    Expect = [{<<"activated_flag">>, true},
              {<<"name_register">>, <<"Ryan Zezeski">>},
              {<<"phones_set">>, <<"555-5555">>},
              {<<"phones_set">>, <<"867-5309">>},
              {<<"page_views_counter">>, <<"1502">>},
              {<<"events_map.RICON_register">>, <<"spoke">>},
              {<<"events_map.Surge_register">>, <<"attended">>}],

    valid_extraction(Result, Expect).

field_separator_test() ->
    MapBin = binary_crdt(map),
    Result = yz_dt_extractor:extract(MapBin, [{field_separator, <<"--">>}]),
    Expect = [{<<"activated_flag">>, true},
              {<<"name_register">>, <<"Ryan Zezeski">>},
              {<<"phones_set">>, <<"555-5555">>},
              {<<"phones_set">>, <<"867-5309">>},
              {<<"page_views_counter">>, <<"1502">>},
              {<<"events_map--RICON_register">>, <<"spoke">>},
              {<<"events_map--Surge_register">>, <<"attended">>}],

    valid_extraction(Result, Expect).


valid_extraction(Result, Expect) ->
    ?assertEqual((length(Expect)), (length(Result))),
    Pairs = lists:zip(lists:sort(Expect), lists:sort(Result)),
    [ ?assertEqual(E,R) || {E,R} <- Pairs],
    ?STACK_IF_FAIL(yz_solr:prepare_json([{doc, Result}])).

binary_crdt(Type) ->
    riak_kv_crdt:to_binary(raw_type(Type)).

raw_type(map) ->
    ?MAP_TYPE(
       element(2,?MAP_TYPE:update(
         {update,
          [
           {update, {<<"activated">>, ?FLAG_TYPE}, enable},
           {update, {<<"name">>, ?REG_TYPE}, {assign, <<"Ryan Zezeski">> }},
           {update, {<<"phones">>, ?SET_TYPE}, {add_all, [<<"555-5555">>, <<"867-5309">>]}},
           {update, {<<"page_views">>, ?COUNTER_TYPE}, {increment, 1502}},
           {update, {<<"events">>, ?MAP_TYPE},
            {update,
             [
              {update, {<<"RICON">>, ?REG_TYPE}, {assign, <<"spoke">>}},
              {update, {<<"Surge">>, ?REG_TYPE}, {assign, <<"attended">>}}
             ]}}
          ]}, <<0>>, ?MAP_TYPE:new())));
raw_type(set) ->
    ?SET_TYPE(
       element(2,?SET_TYPE:update({add_all, [<<"Riak">>, <<"Cassandra">>, <<"Voldemort">>]},
                                  <<0>>, ?SET_TYPE:new()))
      );
raw_type(counter) ->
    ?COUNTER_TYPE(
       element(2,?COUNTER_TYPE:update({increment, 10}, <<0>>, ?COUNTER_TYPE:new()))).
