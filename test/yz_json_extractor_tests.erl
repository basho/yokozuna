-module(yz_json_extractor_tests).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

json_extract_test() ->
    {ok, TestJSON} = file:read_file("../test/test.json"),
    Result = yz_json_extractor:extract(TestJSON),
    Expect =
        [{<<"name">>,<<"ryan">>},
         {<<"age">>,29},
         {<<"pets">>,<<"smokey">>},
         {<<"pets">>,<<"bandit">>},
         {<<"books_title">>,<<"Introduction to Information Retrieval">>},
         {<<"books_title">>,<<"Principles of Distributed Database Systems">>},
         {<<"books_authors">>,<<"Christopher D. Manning">>},
         {<<"books_authors">>,<<"Prabhakar Raghavan">>},
         {<<"books_authors">>,<<"Hinrich Schütze">>},
         {<<"books_authors">>,<<"M. Tamer Özsu">>},
         {<<"books_authors">>,<<"Patrick Valduriez">>},
         {<<"alive">>,true},
         {<<"married">>,false},
         {<<"a_number">>,1.1e6},
         {<<"lucky_numbers">>,13},
         {<<"lucky_numbers">>,17},
         {<<"lucky_numbers">>,21}],

    %% Do one at a time so failure is easier to understand
    ?assertEqual(length(Expect), length(Result)),
    Pairs = lists:zip(lists:sort(Expect), lists:sort(Result)),
    [?assertEqual(E,R) || {E,R} <- Pairs].
