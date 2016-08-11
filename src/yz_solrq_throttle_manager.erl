%% -------------------------------------------------------------------
%% Copyright (c) 2016 Basho Technologies, Inc. All Rights Reserved.
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
-module(yz_solrq_throttle_manager).

%% API
-export([create_table/0, update_counter/2, get_index_length/1]).
-define(WEIGHT, 0.66).

create_table() ->
    Options = [named_table,
               set,
               public,
               {read_concurrency, true},
               {write_concurrency, true}],
    ets:new(yz_solrq_lengths, Options).

update_counter({_Index, _Partition} = IdxPart, CurrentLength) ->
    Old = try
              ets:lookup_element(yz_solrq_lengths, IdxPart, 2)
          catch
              error:badarg ->
                  0
          end,
    New = round((1-?WEIGHT) * Old + ?WEIGHT * CurrentLength),
    ets:insert(yz_solrq_lengths, {IdxPart, New}).

get_index_length(Index) ->
    Values = ets:select(yz_solrq_lengths, [{{{'$1','$2'},'$3'},[{'==','$1',{const,Index}}],['$3']}]),
    NumQueues = length(Values),
    case NumQueues of
        0 ->
            0;
        _ ->
            lists:sum(Values) div NumQueues
    end.


