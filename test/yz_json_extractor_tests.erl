-module(yz_json_extractor_tests).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(STACK_IF_FAIL(Expr),
        ?IF(try
                Expr, true
            catch _:_ ->
                    false
            end,
            ok,
            begin
                Trace = erlang:get_stacktrace(),
                ?debugFmt("~n~p failed: ~p~n", [??Expr, Trace]),
                throw({expression_failed, ??Expr})
            end)).

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

    ?assertEqual(length(Expect), length(Result)),
    Pairs = lists:zip(lists:sort(Expect), lists:sort(Result)),
    %% Assert one field at a time so failure is easier to understand
    [?assertEqual(E,R) || {E,R} <- Pairs],
    %% Verify conversion doesn't error
    ?STACK_IF_FAIL(yz_solr:prepare_json([{doc, Result}])).
