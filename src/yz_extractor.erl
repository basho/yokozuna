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

-module(yz_extractor).
-compile(export_all).
-include("yokozuna.hrl").

-type register_opts() :: [overwrite].
-type get_def_opts() :: [check_default].

%% NOTE: The map is treated as an orddict so these entries must be
%%       ordered correctly or `get_def' may report no extractor is
%%       registered when there is one.
-define(DEFAULT_MAP, [{default, yz_noop_extractor},
                      {"application/json",yz_json_extractor},
                      {"application/riak_counter", yz_dt_extractor},
                      {"application/riak_map", yz_dt_extractor},
                      {"application/riak_set", yz_dt_extractor},
                      {"application/xml",yz_xml_extractor},
                      {"text/plain",yz_text_extractor},
                      {"text/xml",yz_xml_extractor}
                     ]).
-define(META_EXTRACTOR_MAP, yokozuna_extractor_map).


%% @doc Get the extractor definition registered for the given
%%      `MimeType'.  Return `none' if there is nothing registered.
-spec get_def(mime_type()) -> extractor_def() | none.
get_def(MimeType) ->
    get_def(MimeType, [check_default]).

%% @doc The same as `get_def/1' but allows options to be passed.
%%
%% Options:
%%
%%   `check_default' - If no entry can be found for the given
%%                     `MimeType' then check for a default extractor.
%%
-spec get_def(mime_type(), get_def_opts()) -> extractor_def() | none.
get_def(MimeType, Opts) ->
    Map = get_map(),
    get_def(Map, MimeType, Opts).

%% @doc The same as `get_def/2' but takes the extractor `Map' as
%%      argument instead of fetching it.
-spec get_def(extractor_map(), mime_type(), get_def_opts()) ->
                     extractor_def() | none.
get_def(Map, MimeType, Opts) ->
    CheckDefault = proplists:get_bool(check_default, Opts),
    case orddict:find(MimeType, Map) of
        {ok, {_, _}=ModuleOpts} -> ModuleOpts;
        {ok, Module} -> Module;
        error ->
            if CheckDefault -> get_default(Map);
               true -> none
            end
    end.

%% @doc Get the default entry from the extractor `Map' or return
%%      `none' if there isn't one.
-spec get_default(extractor_map()) -> extractor_def() | none.
get_default(Map) ->
    case orddict:find(default, Map) of
        {ok, Def} -> Def;
        error -> none
    end.

%% @doc Get the extractor map.
-spec get_map() -> extractor_map().
get_map() ->
    get_map(yz_misc:get_ring(transformed)).

%% @doc Like `get_map/0' but takes the `Ring' as argument instead of
%%      fetching it.
-spec get_map(ring()) -> extractor_map().
get_map(Ring) ->
    case riak_core_ring:get_meta(?META_EXTRACTOR_MAP, Ring) of
        {ok, Map} -> Map;
        undefined -> ?DEFAULT_MAP
    end.

%% @doc Check if there is an entry for the given extractor `Name'.
-spec is_registered(extractor_name()) -> boolean().
is_registered(ExtractorName) ->
    Map = get_map(),
    F = fun({_, {Name, _}}) ->
                Name == ExtractorName;
           ({_, Name}) ->
                Name == ExtractorName
        end,
    lists:any(F, Map).

%% @doc Register an extractor `Def' under the given `MimeType'.  If
%%      there is already an entry then return `already_registered'.
%%      The `register/2' call can be used to overwrite an entry.
-spec register(mime_type(), extractor_def()) ->
                      extractor_map() | already_registered.
register(MimeType, Def) ->
    ?MODULE:register(MimeType, Def, []).

%% @doc The same as `register/2' but allows options to be passed.
%%
%% Options:
%%
%%   `overwrite' - Overwrite the entry if one already exists.
%%
-spec register(mime_type(), extractor_def(), register_opts()) ->
                      extractor_map() | already_registered.
register(MimeType, Def, Opts) ->
    case yz_misc:set_ring_meta(?META_EXTRACTOR_MAP,
                               ?DEFAULT_MAP,
                               fun register_map/2,
                               {MimeType, Def, Opts}) of
        {ok, Ring} ->
            get_map(Ring);
        not_changed ->
            already_registered
    end.

register_map(Map, {MimeType, Def, Opts}) ->
    CurrentDef = get_def(Map, MimeType, []),
    Overwrite = proplists:get_bool(overwrite, Opts),
    case {CurrentDef, Overwrite} of
        {none, _} ->
            orddict:store(MimeType, Def, Map);
        {_, true} ->
            orddict:store(MimeType, Def, Map);
        {_, false} ->
            ignore
    end.

%% @doc Run the extractor def against the `Value' to produce a list of
%%      fields.
-spec run(binary(), extractor_def()) -> fields() | {error, any()}.
run(Value, {Module, Opts}) ->
    Module:extract(Value, Opts);
run(Value, Module) ->
    Module:extract(Value).
