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
-module(yz_perf).

%% API
-export([resize_workers/1, resize_helpers/1]).


%% @doc Resize the number of queues. For debugging/testing only,
%%      this will briefly cause the worker that queues remap to
%%      to change so updates may be out of order briefly.
-spec resize_workers(NewSize :: pos_integer()) -> yz_solrq:size_resps().
resize_workers(NewSize) when NewSize > 0 ->
    do_child_resize(NewSize, yz_solrq:num_worker_specs(), worker).

%% @doc Resize the number of helpers. For debugging/testing only,
%%      this will briefly cause the worker that queues remap to
%%      to change so updates may be out of order briefly.
-spec resize_helpers(NewSize :: pos_integer()) -> yz_solrq:size_resps().
resize_helpers(NewSize) when NewSize > 0 ->
    do_child_resize(NewSize, yz_solrq:num_helper_specs(), helper).

-spec do_child_resize(NewSize :: pos_integer(),
        OldSize :: non_neg_integer(),
        ChildType :: helper | worker)
            -> yz_solrq:size_resps().
do_child_resize(OldSize, OldSize, _ChildType) ->
    same_size;
do_child_resize(NewSize, OldSize, ChildType) when NewSize < OldSize->
    %% Reduce down to the new size before killing
    set_tuple(ChildType, NewSize),
    lists:foreach(fun(I) ->
                     Name = reg_name(ChildType, I),
                     _ = supervisor:terminate_child(yz_solrq_sup, Name),
                     ok = supervisor:delete_child(yz_solrq_sup, Name)
                  end,
                  lists:seq(NewSize + 1, OldSize)),
    {shrank, OldSize - NewSize};
do_child_resize(NewSize, OldSize, ChildType) ->
    lists:foreach(fun(I) ->
                        supervisor:start_child(yz_solrq_sup,
                        yz_solrq_sup:child_spec(ChildType, reg_name(ChildType, I)))
                  end,
                  lists:seq(OldSize + 1, NewSize)),
    set_tuple(ChildType, NewSize),
    {grew, NewSize - OldSize}.

set_tuple(helper, Size) ->
    yz_solrq:set_solrq_helper_tuple(Size);
set_tuple(worker, Size) ->
    yz_solrq:set_solrq_worker_tuple(Size).

reg_name(helper, Name) ->
    yz_solrq:helper_regname(Name);
reg_name(worker, Name) ->
    yz_solrq:worker_regname(Name).


