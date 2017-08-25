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

%% @doc Get the `extractor_map' from the mochiglobal cache or
%% check and read through CMD. If not in CMD, it will check
%% the Ring's metadata or return a default map.
-spec get_map() -> extractor_map().
get_map() ->
    case mochiglobal:get(?META_EXTRACTOR_MAP, undefined) of
        undefined ->
            Map = get_map_read_through(),
            mochiglobal:put(?META_EXTRACTOR_MAP, Map),
            Map;
        Map ->
            Map
    end.

%% @doc Like `get_map/0' but takes the `Ring' as argument instead
%%      and checks the Ring's metadata. If the map is not in the Ring's
%%      metadata, then return the default map.
-spec get_map(ring()) -> extractor_map().
get_map(Ring) ->
    case riak_core_ring:get_meta(?META_EXTRACTOR_MAP, Ring) of
        {ok, Map} -> Map;
        undefined -> ?DEFAULT_MAP
    end.

%% @doc Check if there is an entry for the given extractor `Name'.
-spec is_registered(extractor_def()) -> boolean().
is_registered({ExtractorName, _}) ->
    is_registered(ExtractorName);
is_registered(ExtractorName) ->
    Map = get_map(),
    F = fun({_, {Name, _}}) ->
                Name == ExtractorName;
           ({_, Name}) ->
                Name == ExtractorName
        end,
    lists:any(F, Map).

%% @doc Register an extractor `Def' under the given `MimeType'.  If
%%      there is already an entry and overwrite is false, then return
%%      `already_registered'. If there is no option to overwrite an existing
%%      entry then return `not_set_to_overwrite'.
%%      The `register/2' call can be used to overwrite an entry.
-spec register(mime_type(), extractor_def()) ->
                      extractor_map() | already_registered | not_set_to_overwrite.
register(MimeType, Def) ->
    ?MODULE:register(MimeType, Def, []).

%% @doc The same as `register/2' but allows options to be passed.
%%
%% Options:
%%
%%   `overwrite' - Overwrite the entry if one already exists.
%%
-spec register(mime_type(), extractor_def(), register_opts()) ->
                      extractor_map() | already_registered | not_set_to_overwrite.
register(MimeType, Def, Opts) ->
    case register_map(get_map(), {MimeType, Def, Opts}) of
        registered ->
            already_registered;
        ignore ->
            not_set_to_overwrite;
        Map ->
            ok = riak_core_metadata:put(?YZ_META_EXTRACTORS, ?META_EXTRACTOR_MAP,
                                        Map),
            mochiglobal:put(?META_EXTRACTOR_MAP, Map),
            Map
    end.

%% @doc Register a map with a new `MimeType'-`Def' key-value or overwrite
%% an existing one if `overwrite' is set to true. If `overwrite' is false and
%% the `ExtractorName' is_registered, then return `registered'. If it is not
%% registered and `overwrite' is false, then just return `ignore'.
-spec register_map(extractor_map(), {mime_type(), extractor_def(), register_opts()})
                  -> extractor_map() | registered | ignore.
register_map(Map, {MimeType, Def, Opts}) ->
    CurrentDef = get_def(Map, MimeType, []),
    Overwrite = proplists:get_bool(overwrite, Opts),
    case {CurrentDef, is_registered(CurrentDef), Overwrite} of
        {none, _, _} ->
            orddict:store(MimeType, Def, Map);
        {_, _, true} ->
            orddict:store(MimeType, Def, Map);
        {_, true, false} ->
            registered;
        {_, false, false} ->
            ignore
    end.

%% @doc Run the extractor def against the `Value' to produce a list of
%%      fields.
-spec run(binary(), extractor_def()) -> fields() | {error, any()}.
run(Value, {Module, Opts}) ->
    Module:extract(Value, Opts);
run(Value, Module) ->
    Module:extract(Value).

%% @doc If there's no `extractor_map' in the cache, then call this read-through
%%      function which checks if the map has been moved to CMD, which is also
%%      registered as a riak_core_capability. If it's not in CMD, then it will
%%      pull the map from the Ring's metadata (or just the default map) and
%%      update CMD accordingly.
-spec get_map_read_through() -> extractor_map().
get_map_read_through() ->
    ExtractorMetaMap = riak_core_metadata:get(?YZ_META_EXTRACTORS, ?META_EXTRACTOR_MAP),
    ExtractorCap = riak_core_capability:get(?YZ_CAPS_CMD_EXTRACTORS),
    case {ExtractorMetaMap /= undefined, ExtractorCap} of
        {true, true} ->
            ExtractorMetaMap;
        {true, false} ->
            ExtractorMetaMap;
        {_, _} ->
            ExtractorRingMap = get_map(yz_misc:get_ring(transformed)),
            ok = riak_core_metadata:put(?YZ_META_EXTRACTORS, ?META_EXTRACTOR_MAP,
                                       ExtractorRingMap),
            ExtractorRingMap
    end.
