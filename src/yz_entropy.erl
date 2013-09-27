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
-module(yz_entropy).
-compile(export_all).
-include("yokozuna.hrl").

%% @doc This module contains functionality related to entropy.

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Iterate all the entropy data in `Index' calling `Fun' for
%%      every 100 entries.
-spec iterate_entropy_data(index_name(), list(), function()) -> ok.
iterate_entropy_data(Index, Filter, Fun) ->
    case yz_solr:ping(Index) of
        true ->
            Filter2 = [{continuation, none},
                       {limit, 100}|Filter],
            ED = yz_solr:entropy_data(Index, Filter2),
            iterate_entropy_data(Index, Filter2, Fun, ED);
        false ->
            ok
    end.

iterate_entropy_data(Index, Filter, Fun, #entropy_data{more=true,
                                                       continuation=Cont,
                                                       pairs=Pairs}) ->
    lists:foreach(Fun, Pairs),
    Filter2 = lists:keyreplace(continuation, 1, Filter, {continuation, Cont}),
    ED = yz_solr:entropy_data(Index, Filter2),
    iterate_entropy_data(Index, Filter2, Fun, ED);
iterate_entropy_data(_, _, Fun, #entropy_data{more=false,
                                              pairs=Pairs}) ->
    lists:foreach(Fun, Pairs).
