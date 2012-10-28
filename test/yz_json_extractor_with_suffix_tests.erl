-module(yz_json_extractor_with_suffix_tests).

-include_lib("eunit/include/eunit.hrl").

json_extract_test() ->
    %% Actual
    {ok, TestJSON} = file:read_file("../test/with_suffix.json"),
    Fields = yz_json_extractor_with_suffix:extract(TestJSON),
    Xml = yz_solr:prepare_xml([{doc, Fields}]),
    %% Expected
    {ok, TestXML} = file:read_file("../test/with_suffix.xml"),
    Xml1 = binary:replace(TestXML, <<"\n">>, <<"">>, [global]),
    ?assertEqual(Xml1, iolist_to_binary(Xml)).
