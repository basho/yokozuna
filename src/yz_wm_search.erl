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

-module(yz_wm_search).
-compile(export_all).
-include_lib("webmachine/include/webmachine.hrl").

init(_) ->
    {ok, none}.

allowed_methods(Req, S) ->
    Methods = ['GET'],
    {Methods, Req, S}.

content_types_provided(Req, S) ->
    Types = [{"text/xml", search}],
    {Types, Req, S}.

search(Req, S) ->
    Index = wrq:path_info(index, Req),
    %% Query = wrq:get_qs_value("q", Req),
    Params = wrq:req_qs(Req),
    Mapping = yz_events:get_mapping(),
    XML = yz_solr:search(Index, Params, Mapping),
    Req2 = wrq:set_resp_header("Content-Type", "text/xml", Req),
    {XML, Req2, S}.
