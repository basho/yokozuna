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

-module(yz_wm_extract).
-compile(export_all).
-include("yokozuna.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(state, {
          content :: binary(),
          extractor_name :: extractor_name()
         }).

routes() ->
    [{["search", "extract"], yz_wm_extract, []}].

init(_) ->
    {ok, #state{}}.

allowed_methods(RD, S) ->
    Methods = ['PUT'],
    {Methods, RD, S}.

malformed_request(RD, S) ->
    E = to_atom(wrq:get_req_header(?YZ_HEAD_EXTRACTOR, RD)),
    CT = wrq:get_req_header(?HEAD_CTYPE, RD),
    Content = wrq:req_body(RD),

    case {E, CT} of
        {undefined, undefined} ->
            RD2 = missing_headers(RD),
            {true, RD2, S};
        _ ->
            case size(Content) of
                0 ->
                    RD2 = no_content(RD),
                    {true, RD2, S};
                _ ->
                    {false, RD, S#state{content=Content}}
            end
    end.

%% Accept the mime-type provided by user, if one is not provided use
%% extractor specified in header.
content_types_accepted(RD, S) ->
    E = to_atom(wrq:get_req_header(?YZ_HEAD_EXTRACTOR, RD)),
    CT = wrq:get_req_header(?HEAD_CTYPE, RD),

    case {E, CT} of
        {undefined, _} ->
            %% No extractor set, use content-type.
            case yz_extractor:get_def(CT, []) of
                none ->
                    RD2 = no_extractor_registered(RD, CT),
                    {{halt, 500}, RD2, S};
                E2 ->
                    S2 = S#state{extractor_name=E2},
                    {[{CT, extract}], RD, S2}
            end;

        {_, undefined} ->
            %% Extractor set but no content-type, if no content-type
            %% is specified then webmachine will default to
            %% application/octet-stream.
            check_if_registered_set_ct(RD, S, E, "application/octet-stream");

        {_, _} ->
            %% Both extractor and content-type are set, prefer the
            %% extractor.
            check_if_registered_set_ct(RD, S, E, CT)
    end.

forbidden(RD, S) ->
    {riak_kv_wm_utils:is_forbidden(RD, riak_search), RD, S}.

extract(RD, S) ->
    case yz_extractor:run(S#state.content, S#state.extractor_name) of
        {error, Reason} ->
            RD2 = add_msg(RD, Reason),
            {{halt, 500}, RD2, S};
        Fields ->
            Body = mochijson2:encode({struct, Fields}),
            RD2 = wrq:set_resp_header(?HEAD_CTYPE, "application/json", RD),
            RD3 = wrq:set_resp_body(Body, RD2),
            {Body, RD3, S}
    end.

check_if_registered_set_ct(RD, S, ExtractorName, ContentType) ->
    case yz_extractor:is_registered(ExtractorName) of
        false ->
            RD2 = unknown_extractor(RD, ExtractorName),
            {{halt, 500}, RD2, S};
        true ->
            S2 = S#state{extractor_name=ExtractorName},
            {[{ContentType, extract}], RD, S2}
    end.

missing_headers(RD) ->
    Msg = io_lib:format("the ~s or ~s header must be set~n",
                        [?HEAD_CTYPE, ?YZ_HEAD_EXTRACTOR]),
    add_msg(RD, Msg).

unknown_extractor(RD, E) ->
    Msg = io_lib:format("no extractor registered with the name ~s~n", [E]),
    add_msg(RD, Msg).

no_content(RD) ->
    Msg = io_lib:format("no content was given~n", []),
    add_msg(RD, Msg).

no_extractor_registered(RD, CT) ->
    Msg = io_lib:format("no extractor registered for content-type ~s~n", [CT]),
    add_msg(RD, Msg).

add_msg(RD, Msg) ->
    RD2 = wrq:append_to_response_body(Msg, RD),
    wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD2).

-spec to_atom(undefined | string()) -> atom().
to_atom(undefined) ->
    undefined;
to_atom(S) ->
    list_to_atom(S).
