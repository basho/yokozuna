%% Macros and functions to shared across tests.
-include_lib("eunit/include/eunit.hrl").

-define(STACK_IF_FAIL(Expr),
        try
            _ = Expr, ok
        catch _:_ ->
                Trace = erlang:get_stacktrace(),
                ?debugFmt("~n~p failed: ~p~n", [??Expr, Trace]),
                throw({expression_failed, ??Expr})
        end).

%% A replacement for ?assertEqual that prints the entire binary so
%% that bytes can be compared in case of mismatch.
-define(assertPairsEq(S1,S2),
        ?IF(begin
                ?assertEqual(element(1, S1), element(1, S2)),
                element(2, S1) =:= element(2, S2)
            end,
            ok,
            begin
                Field = element(1, S1),
                ?debugFmt("~nfields not equal: ~s~n", [Field]),
                ?debugFmt("expected: ~p~n", [element(2,S1)]),
                ?debugFmt("actual: ~p~n", [element(2,S2)]),
                throw(pairs_not_equal)
            end)).
