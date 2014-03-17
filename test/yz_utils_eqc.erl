%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Quickcheck test module for `yz_misc'.

-module(yz_utils_eqc).

-ifdef(EQC).

-include("yokozuna.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    Time = 8,
    [
     {timeout, Time*4, ?_assertEqual(true,
                                     eqc:quickcheck(eqc:testing_time(Time,?QC_OUT(prop_compress()))))}
    ].

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_compress() ->
    ?FORALL(S, utf8_string(),
            yz_misc:decompress(yz_misc:compress(S)) == [S]).

utf8_string() ->
    ?LET(X, not_empty(list(utf8_char())), unicode:characters_to_binary(X)).

not_empty(G) ->
    ?SUCHTHAT(X, G, X /= [] andalso X /= <<>> andalso X /=<<"/">>).

utf8_char() ->
    choose(16#0000, 16#00FF).

%%====================================================================
%% Helpers
%%====================================================================

-endif.
