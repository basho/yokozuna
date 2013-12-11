%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc An extractor for Riak KV's datatypes. Maps have their fields
%% concatenated with `field_separator'. Sets are converted into
%% multi-valued fields. Counters are treated as integer fields.
%% Embedded flags are treated as boolean fields. Embedded registers
%% are treated as regular fields.
%%
%% Counter example:
%%
%%    [{<<"counter">>, <<"5">>}]
%%
%% Set example:
%%
%%    [<<"Cassandra">>, <<"Riak">>, <<"Voldemort">>]
%%
%%    [{<<"set">>, <<"Cassandra">>},
%%     {<<"set">>, <<"Riak">>},
%%     {<<"set">>, <<"Voldemort">>}]
%%
%% Map example (note the output of embedded types and conversion of
%% module to symbolic type):
%%
%%    [{{<<"activated">>, riak_dt_od_flag}, true},
%%     {{<<"name">>, riak_dt_lwwreg}, <<"Ryan Zezeski">>},
%%     {{<<"phones">>, riak_dt_orswot}, [<<"555-5555">>, <<"867-5309">>]},
%%     {{<<"page_views">>, riak_dt_pncounter}, 1502},
%%     {{<<"events">>, riak_dt_map},
%%      [{{<<"RICON">>, riak_dt_lwwreg}, <<"spoke">>},
%%       {{<<"Surge">>, riak_dt_lwwreg}, <<"attended">>}]}]
%%
%%    [{<<"activated_flag">>, true},
%%     {<<"name_register">>, <<"Ryan Zezeski">>},
%%     {<<"phones_set">>, <<"555-5555">>},
%%     {<<"phones_set">>, <<"867-5309">>},
%%     {<<"page_views_counter">>, <<"1502">>},
%%     {<<"events_map.RICON_register">>, <<"spoke">>},
%%     {<<"events_map.Surge_register">>, <<"attended">>}]
%%
%% Options:
%%
%%   `field_separator' - Use a different field separator than the
%%                       default of `.'.

-module(yz_dt_extractor).
-compile(export_all).
-include("yokozuna.hrl").
-include_lib("riak_kv/include/riak_kv_types.hrl").
-define(DEFAULT_FIELD_SEPARATOR, <<".">>).

-record(state, {
          fields = [],
          field_separator = ?DEFAULT_FIELD_SEPARATOR
         }).
-type state() :: #state{}.
-type field_path_name() :: undefined | {binary() | undefined, binary()}.
-type datatype() :: map | set | counter | register | flag.
-spec extract(binary()) -> fields() | {error, any()}.
extract(Value) ->
    extract(Value, ?NO_OPTIONS).

-spec extract(binary(), proplist()) -> fields() | {error, any()}.
extract(Value0, Opts) ->
    Sep = proplists:get_value(field_separator, Opts, ?DEFAULT_FIELD_SEPARATOR),
    case riak_kv_crdt:from_binary(Value0) of
        {ok, Value} ->
            extract_fields(Value, #state{field_separator = Sep});
        Err -> Err
    end.

-spec extract_fields(#crdt{}, state()) -> fields().
extract_fields(#crdt{mod=Mod, value=Data}, S) ->
    case extract_fields(undefined, riak_kv_crdt:from_mod(Mod), Mod:value(Data), S) of
        #state{fields=F} -> F;
        Err -> Err
    end.

-spec extract_fields(field_path_name(), datatype(), term(), state()) -> state().
extract_fields(Name0, map, Pairs, #state{field_separator=Sep}=State) ->
    Name = field_name(Name0, map, Sep),
    lists:foldl(extract_map(Name), State, Pairs);
extract_fields(Name0, set, Entries, #state{field_separator=Sep}=State) ->
    Name = field_name(Name0, set, Sep),
    lists:foldl(extract_set(Name), State, Entries);
extract_fields(Name, counter, Value, #state{fields=Fields, field_separator=Sep}=State) ->
    FieldName = field_name(Name, counter, Sep),
    State#state{fields=[{FieldName, ?INT_TO_BIN(Value)}|Fields]};
extract_fields(Name, register, Value, #state{fields=Fields, field_separator=Sep}=State) ->
    FieldName = field_name(Name, register, Sep),
    State#state{fields=[{FieldName, Value}|Fields]};
extract_fields(Name, flag, Value, #state{fields=Fields, field_separator=Sep}=State) ->
    FieldName = field_name(Name, flag, Sep),
    State#state{fields=[{FieldName, Value}|Fields]}.

-spec field_name(field_path_name(), datatype(), binary()) -> binary().
field_name(undefined, map, _Sep) ->
    %% (timeforthat)
    undefined;
field_name(undefined, Type, _Sep) ->
    ?ATOM_TO_BIN(Type);
field_name({undefined, Name}, Type, _Sep) ->
    %% First-level in map
    <<Name/binary,$_,(?ATOM_TO_BIN(Type))/binary>>;
field_name({Prefix, Name}, Type, Sep) ->
    <<Prefix/binary, Sep/binary, Name/binary,$_,(?ATOM_TO_BIN(Type))/binary>>.

-spec extract_set(binary()) -> fun((binary(), state()) -> state()).
extract_set(Name) ->
    fun(Elem, #state{fields=Fields}=Acc) ->
            Acc#state{fields=[{Name, Elem}|Fields]}
    end.

-spec extract_map(field_path_name()) -> fun(({{binary(), module()}, term()}, state()) -> state()).
extract_map(Prefix) ->
    fun({{FieldName, Mod}, Value}, Acc) ->
            Type = riak_kv_crdt:from_mod(Mod),
            extract_fields({Prefix, FieldName}, Type, Value, Acc)
    end.
