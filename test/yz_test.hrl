%% -*- encoding: utf-8 -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013,2015 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% Macros and functions to shared across tests.
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
