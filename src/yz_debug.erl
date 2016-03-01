%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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
-module(yz_debug).

-compile(export_all).
-export([]).

-include("yokozuna.hrl").

solrq_indexq(Index, IndexQ) ->
    [
        {index, Index}
        % , {queue, queue:to_list(element(2, IndexQ))}
        , {queue_len, element(3, IndexQ)}
        , {pending_helper, element(5, IndexQ)}
        , {aux_queue_len, queue:len(element(9, IndexQ))}
        , {draining, element(10, IndexQ)}
        , {fuse_blown, element(11, IndexQ)}
        , {in_flight_len, element(12, IndexQ)}
    ].

solrq_state(State) ->
    [
        {
            indexqs,
            dict:fold(
                fun(Index, IndexQ, Accum) ->
                    [solrq_indexq(Index, IndexQ) | Accum]
                end,
                [],
                element(2, State)
            )
        },
        {num_indexqs, dict:size(element(2, State))},
        {all_queue_len, element(3, State)},
        {queue_hwm, element(4, State)},
        {drain_info, element(6, State)}
    ].

solrqs() ->
    [{Id, solrq_state(sys:get_state(Id))} || Id <- yz_solrq_sup:solrq_names()].

solrq_summary() ->
    Solrqs = solrqs(),
    [
        {num_solrqs, application:get_env(?YZ_APP_NAME, ?SOLRQ_WORKER_CNT, 10)},
        {num_solrq_helpers, application:get_env(?YZ_APP_NAME, ?SOLRQ_HELPER_CNT,
                                                10)},
        {all_queue_len, all_queue_len(Solrqs)},
        {draining_solrqs, draining_solrqs(Solrqs)},
        {draining_indexqs, draining_indexqs(Solrqs)},
        {blown_indexqs, blown_indexqs(Solrqs)},
        {nonzero_queue_lengths, nonzero_queue_lengths(Solrqs)},
        {in_flight_lengths, in_flight_lengths(Solrqs)},
        {nonzero_aux_queue_lengths, nonzero_aux_queue_lengths(Solrqs)}
    ].



all_queue_len(Solrqs) ->
    lists:foldr(
        fun({Id, State}, Accum) ->
            [{Id, proplists:get_value(all_queue_len, State)} | Accum]
        end,
        [],
        Solrqs
    ).

draining_solrqs(Solrqs) ->
    lists:foldr(
        fun({Id, State}, Accum) ->
            case proplists:get_value(drain_info, State) of
                undefined ->
                    Accum;
                DrainInfo ->
                    [{Id, DrainInfo} | Accum]
            end
        end,
        [],
        Solrqs
    ).

draining_indexqs(Solrqs) ->
    lists:foldr(
        fun({Id, State}, SolrqAccum) ->
            IndexQs = lists:foldr(
                fun(IndexQ, IndexQAccum) ->
                    case proplists:get_value(draining, IndexQ) of
                        false ->
                            IndexQAccum;
                        Value ->
                            [{proplists:get_value(index, IndexQ), Value} | IndexQAccum]
                    end
                end,
                [],
                proplists:get_value(indexqs, State)
            ),
            case IndexQs of
                [] ->
                    SolrqAccum;
                _ ->
                    [{Id, IndexQs} | SolrqAccum]
            end
        end,
        [],
        Solrqs
    ).

blown_indexqs(Solrqs) ->
    lists:foldr(
        fun({Id, State}, SolrqAccum) ->
            IndexQs = lists:foldr(
                fun(IndexQ, IndexQAccum) ->
                    case proplists:get_value(fuse_blown, IndexQ) of
                        false ->
                            IndexQAccum;
                        _ ->
                            [proplists:get_value(index, IndexQ) | IndexQAccum]
                    end
                end,
                [],
                proplists:get_value(indexqs, State)
            ),
            case IndexQs of
                [] ->
                    SolrqAccum;
                _ ->
                    [{Id, IndexQs} | SolrqAccum]
            end
        end,
        [],
        Solrqs
    ).

nonzero_queue_lengths(Solrqs) ->
    lists:foldr(
        fun({Id, State}, {SolrqLenAccum, SolrqAccum}) ->
            {IndexQLen, IndexQs} = lists:foldr(
                fun(IndexQ, {IndexQLenAccum, IndexQAccum}) ->
                    case proplists:get_value(queue_len, IndexQ) of
                        0 ->
                            {IndexQLenAccum, IndexQAccum};
                        QueueLen ->
                            {IndexQLenAccum + QueueLen, [{proplists:get_value(index, IndexQ), QueueLen} | IndexQAccum]}
                    end
                end,
                {0, []},
                proplists:get_value(indexqs, State)
            ),
            case IndexQs of
                [] ->
                    {SolrqLenAccum, SolrqAccum};
                _ ->
                    {SolrqLenAccum + IndexQLen, [{Id, IndexQs} | SolrqAccum]}
            end
        end,
        {0, []},
        Solrqs
    ).

in_flight_lengths(Solrqs) ->
    lists:foldr(
        fun({Id, State}, {SolrqLenAccum, SolrqAccum}) ->
            {IndexQLen, IndexQs} = lists:foldr(
                fun(IndexQ, {IndexQLenAccum, IndexQAccum}) ->
                    case proplists:get_value(in_flight_len, IndexQ) of
                        0 ->
                            {IndexQLenAccum, IndexQAccum};
                        QueueLen ->
                            {IndexQLenAccum + QueueLen, [{proplists:get_value(index, IndexQ), QueueLen} | IndexQAccum]}
                    end
                end,
                {0, []},
                proplists:get_value(indexqs, State)
            ),
            case IndexQs of
                [] ->
                    {SolrqLenAccum, SolrqAccum};
                _ ->
                    {SolrqLenAccum + IndexQLen, [{Id, IndexQs} | SolrqAccum]}
            end
        end,
        {0, []},
        Solrqs
    ).

nonzero_aux_queue_lengths(Solrqs) ->
    lists:foldr(
        fun({Id, State}, {SolrqLenAccum, SolrqAccum}) ->
            {IndexQLen, IndexQs} = lists:foldr(
                fun(IndexQ, {IndexQLenAccum, IndexQAccum}) ->
                    case proplists:get_value(aux_queue_len, IndexQ) of
                        0 ->
                            {IndexQLenAccum, IndexQAccum};
                        QueueLen ->
                            {IndexQLenAccum + QueueLen, [{proplists:get_value(index, IndexQ), QueueLen} | IndexQAccum]}
                    end
                end,
                {0, []},
                proplists:get_value(indexqs, State)
            ),
            case IndexQs of
                [] ->
                    {SolrqLenAccum, SolrqAccum};
                _ ->
                    {SolrqLenAccum + IndexQLen, [{Id, IndexQs} | SolrqAccum]}
            end
        end,
        {0, []},
        Solrqs
    ).
