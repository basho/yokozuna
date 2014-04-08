-module(yz_component_tests).
-compile(export_all).

-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

disable_index_test()->
  yokozuna:disable(index),
  ?assertEqual(yz_kv:index({},delete,{}), ok).

disable_search_test()->
    yokozuna:disable(search),
    {Available, _, _} = yz_wm_search:service_available({},{}),
    ?assertEqual(Available, false),
    Resp = yz_pb_search:process(ignore, ignore),
    ?assertEqual({error, "Search component disabled.", ignore}, Resp).
