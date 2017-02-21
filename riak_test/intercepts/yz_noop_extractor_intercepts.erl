%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%-------------------------------------------------------------------

%% Example from:
%% http://docs.basho.com/riak/latest/dev/search/custom-extractors/#An-Example-Custom-Extractor

-module(yz_noop_extractor_intercepts).
-compile(export_all).
-include("intercept.hrl").

extract_httpheader(Value) ->
    extract_httpheader(Value, []).

extract_httpheader(Value, _Opts) ->
    {ok,
        {http_request,
         Method,
         {absoluteURI, http, Host, undefined, Uri},
         _Version},
        _Rest} = erlang:decode_packet(http, Value, []),
    [{method, Method}, {host, list_to_binary(Host)}, {uri, list_to_binary(Uri)}].

extract_non_unicode_data(Value) ->
    extract_non_unicode_data(Value, []).

extract_non_unicode_data(_Value, _Opts) ->
    [{blob, <<9147374713>>}].
