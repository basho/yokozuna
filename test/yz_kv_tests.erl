-module(yz_kv_tests).
-compile(export_all).

-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

set_index_flag_test()->
  meck:new(riak_core_bucket),
  meck:expect(riak_core_bucket, set_bucket,
    fun(Bucket, Props) when Bucket =:= <<"a">> ->
      case Props of
        [{?YZ_INDEX_CONTENT, true}] -> ok;
        _ -> error
      end
    end),
  ?assertEqual(yz_kv:set_index_flag(<<"a">>), ok),
  ?assert(meck:validate(riak_core_bucket)),
  meck:unload(riak_core_bucket).
