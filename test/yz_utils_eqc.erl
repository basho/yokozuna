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
-define(Q_FUN_OUT, "testing").

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    Time = 2,
    [
     {timeout, Time*4, ?_assertEqual(true,
                                     eqc:quickcheck(eqc:testing_time(Time,?QC_OUT(prop_compress()))))},
     {timeout, Time*4, ?_assertEqual(true,
                                     eqc:quickcheck(eqc:testing_time(Time,?QC_OUT(prop_hostname()))))},
     {timeout, Time*4, ?_assertEqual(true,
                                     eqc:quickcheck(eqc:testing_time(Time,?QC_OUT(prop_dict()))))},
     {timeout, Time*4, ?_assertEqual(true,
                                     eqc:quickcheck(eqc:testing_time(Time,?QC_OUT(prop_queue()))))}

    ].

%% ====================================================================
%% EQC Properties
%% ====================================================================

prop_compress() ->
    ?FORALL(S, utf8_string(),
            yz_misc:decompress(yz_misc:compress(S)) == [S]).

prop_hostname() ->
    ?FORALL({Node, Host}, gen_nodes(),
            (yz_misc:hostname(Node) == atom_to_list(Host))).

prop_dict() ->
    ?FORALL({Key, Dict, Val}, 
            {non_blank_string(), gen_dict(), non_blank_string()},
            begin
                 NewDict = dict:store(Key, Val, Dict),
                 yz_misc:dict_get(Key, NewDict, <<>>) =:= Val
            end).

prop_queue() ->
    ?FORALL(Queue, 
            gen_queue(),
            begin
                F = queue_fun(),
                {First, _Rest} = yz_misc:queue_pop(Queue, F),
                case Queue of
                    [] -> First == ?Q_FUN_OUT;
		    _ ->
                        [H|_T] = Queue,
                        First == H
                end
            end).

%%====================================================================
%% Generators
%%====================================================================

non_blank_string() ->
    ?LET(X, not_empty(list(lower_char())), list_to_binary(X)).

lower_char() ->
    choose(16#20, 16#7f).

utf8_string() ->
    ?LET(X, not_empty(list(utf8_char())), unicode:characters_to_binary(X)).

not_empty(G) ->
    ?SUCHTHAT(X, G, X /= [] andalso X /= <<>> andalso X /=<<"/">>).

utf8_char() ->
    choose(16#0000, 16#00FF).

gen_nodes() ->
    oneof([{'foo@127.0.0.1', '127.0.0.1'}, {'baz@localhost', 'localhost'}, 
           {'bat@bozeman-inc.com', 'bozeman-inc.com'}, {'eddie-vedder@123.32.23.123', '123.32.23.123'}]).

gen_dict() ->
    dict:new().

gen_queue() ->
    list(binary()).

queue_fun() ->
    fun() ->
        [?Q_FUN_OUT]
    end.
-endif.
