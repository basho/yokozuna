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
%% @doc This module contains functionality related to entropy.
-module(yz_entropy).
-export([iterate_entropy_data/3, iterate_entropy_data/4]).
-include("yokozuna.hrl").

%% @doc Iterate all the entropy data in `Index' calling `Fun' for
%%      every 100 entries.
-spec iterate_entropy_data(index_name(), list(), function()) ->
                                  ok|error|not_available.
iterate_entropy_data(Index, Filter, Fun) ->
    yz_solr:with_worker(
      fun(Worker) ->
              iterate_entropy_data(Worker, Index, Filter, Fun)
      end).

iterate_entropy_data(Worker, Index, Filter, Fun) ->
    case yz_solr:ping(Index) of
        true ->
            Filter2 = [{continuation, none},
                       {limit,
                        app_helper:get_env(?YZ_APP_NAME,
                                           entropy_data_limit, 100)}|Filter],
            case get_entropy_data(Worker, Index, Filter2) of
                {ok, ED} ->
                    iterate_entropy_data(Worker, Index, Filter2, Fun, ED);
                {Err, _ED} ->
                    Err
            end;
        _ ->
            ?NOTICE("Can't ping Solr index ~p to start iterating over entropy data", [Index]),
            not_available
    end.

%%%===================================================================
%%% Private
%%%===================================================================

-spec iterate_entropy_data(Worker::pid(), index_name(), list(), function(), ED::entropy_data()) ->
    ok|error.
iterate_entropy_data(Worker, Index, Filter, Fun, #entropy_data{more=true,
                                                               continuation=Cont,
                                                               pairs=Pairs}) ->
    %% apply function to pairs before iterating through the next set
    lists:foreach(Fun, Pairs),
    Filter2 = lists:keyreplace(continuation, 1, Filter, {continuation, Cont}),
    case get_entropy_data(Worker, Index, Filter2) of
        {ok, ED} ->
            iterate_entropy_data(Worker, Index, Filter2, Fun, ED);
        {Err, _ED} ->
            Err
    end;
iterate_entropy_data(_Worker, _, _, Fun, #entropy_data{more=false, pairs=Pairs}) ->
    lists:foreach(Fun, Pairs).

-spec get_entropy_data(Worker::pid(), index_name(), list()) -> {ok|error, entropy_data()}.
get_entropy_data(Worker, Index, Filter) ->
    case yz_solr:entropy_data(Worker, Index, Filter) of
        {error, {error, req_timedout}} ->
            ?ERROR("failed to iterate over entropy data due to request"
                   ++ " exceeding timeout ~b for filter params ~p",
                   [?YZ_SOLR_ED_REQUEST_TIMEOUT, Filter]),
            {error, #entropy_data{more=false, pairs=[]}};
        {error, Err} ->
            ?ERROR("failed to iterate over entropy data due to request"
                   ++ " error ~p for filter params ~p", [Err, Filter]),
            {error, #entropy_data{more=false, pairs=[]}};
        ED ->
            {ok, ED}
    end.
