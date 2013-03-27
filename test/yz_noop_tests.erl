-module(yz_noop_tests).
-compile(export_all).

-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

index_noop_test()->
  yokozuna:noop_flag(index, true),
  ?assertEqual(yz_kv:index({},delete,{}), ok).

search_noop_test()->
  yokozuna:noop_flag(search, true),
  ?assertEqual(yz_wm_search:service_available({},{}), false).
