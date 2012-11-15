%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc An extractor for JSON.  Nested objects have their fields concatenated
%% with `field_separator'.  An array is converted into a multi-valued field.
%% Fields are suffixed to match dynamic field definitions in the default
%% schema.
%%
%% Example:
%%
%%   {"name":"ryan",
%%    "info":{"city":"Baltimore",
%%            "visited":["Boston", "New York", "San Francisco"]}}
%%
%%   [{<<"info_visited_txt">>,<<"San Francisco">>},
%%    {<<"info_visited_txt">>,<<"New York">>},
%%    {<<"info_visited_txt">>,<<"Boston">>},
%%    {<<"info_city_t">>,<<"Baltimore">>},
%%    {<<"name_t">>,<<"ryan">>}]
%%
%% Options:
%%
%%   `field_separator' - Use a different field separator than the
%%                       default of `_'.

-module(yz_json_extractor_with_suffix).

-export([extract/1,
         extract/2]).

-include("yokozuna.hrl").
-define(DEFAULT_FIELD_SEPARATOR, <<"_">>).

-record(state, {fields = [],
                field_separator = ?DEFAULT_FIELD_SEPARATOR,
                multivalue = false
               }).
-type state() :: #state{}.

-spec extract(binary()) -> fields() | {error, any()}.
extract(Value) ->
    extract(Value, ?NO_OPTIONS).

-spec extract(binary(), proplist()) -> fields() | {error, any()}.
extract(Value, Opts) ->
    Sep = proplists:get_value(field_separator, Opts, ?DEFAULT_FIELD_SEPARATOR),
    extract_fields(Value, #state{field_separator=Sep}).

-spec extract_fields(binary(), state()) -> fields().
extract_fields(Value, S) ->
    Struct = mochijson2:decode(Value),
    S2 = extract_fields(undefined, Struct, S),
    S2#state.fields.

extract_fields(Name, Value, State) when is_binary(Value) ->
    extract_field(Name, string, Value, State);

extract_fields(Name, Value, State) when is_number(Value) ->
    Number = if
        is_float(Value) -> float_to_list(Value);
        is_integer(Value) -> integer_to_list(Value)
    end,
    extract_field(Name, number, list_to_binary(Number), State);

extract_fields(Name, Value, State) when is_boolean(Value) ->
    extract_field(Name, boolean, atom_to_binary(Value, utf8), State);

extract_fields(Name, Values, State) when is_list(Values) ->
    Fun = fun(InnerValue, InnerState) ->
            extract_fields(Name, InnerValue, InnerState)
    end,
    State1 = lists:foldl(Fun, State#state{multivalue=true}, Values),
    case State#state.multivalue of
        true ->
            State1;
        false ->
            State1#state{multivalue=false}
    end;

extract_fields(Name, {struct, PropList}, State) ->
    Fun = fun({InnerName, InnerValue}, InnerState) ->
            InnerName1 = concat_field_name(Name, InnerName, State#state.field_separator),
            extract_fields(InnerName1, InnerValue, InnerState)
    end,
    lists:foldl(Fun, State, PropList);

extract_fields(_, _, State) ->
    State.

extract_field(Name, Type, Value, State) ->
    Name1 = field_name(Name, Type, State#state.multivalue),
    Fields = [{Name1, Value}|State#state.fields],
    State#state{fields=Fields}.

field_name(Name, Type, MultiValue) ->
    <<Name/binary, (suffix(Type, MultiValue))/binary>>.

suffix(string, true) -> <<"_txt">>;
suffix(number, true) -> <<"_ds">>;
suffix(boolean, true) -> <<"_bs">>;
suffix(string, _) -> <<"_t">>;
suffix(number, _) -> <<"_d">>;
suffix(boolean, _) -> <<"_b">>.

concat_field_name(undefined, Name, _) ->
    Name;
concat_field_name(Name, Name1, Separator) ->
    <<Name/binary, Separator/binary, Name1/binary>>.
