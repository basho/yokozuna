-module(yz_json_extractor_with_suffix_tests).

-include_lib("eunit/include/eunit.hrl").

json_extract_test() ->
    %% Actual
    {ok, TestJSON} = file:read_file("../test/with_suffix.json"),
    Fields = yz_json_extractor_with_suffix:extract(TestJSON),
    JSON = yz_solr:prepare_json([{doc, Fields}]),
    %% Expected
    {ok, TestJSONExp} = file:read_file("../test/with_suffix_expected.json"),
    JSON1 = binary:replace(TestJSONExp, <<"\n">>, <<"">>, [global]),
    ?assertEqual(JSON1, iolist_to_binary(JSON)).
