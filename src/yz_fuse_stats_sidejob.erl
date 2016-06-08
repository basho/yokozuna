%% -------------------------------------------------------------------
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
%%
%% @doc yz_fuse_stats_sidejob is a module implementing for offloading
%%      statistics updates to a background worker pool maintained by sidejob
%%      so that the calls to exometer don't slow down indexing work.
%%      All operations are pass-through to yz_stat to keep the "how" of stats
%%      updates in one place.

-module(yz_fuse_stats_sidejob).
-behaviour(fuse_stats_plugin).
-export([init/1, increment/2]).

%% @doc Initialize exometer for `Name'.
-spec init(Name :: atom()) -> ok.
init(Name) ->
    yz_stat:initialize_fuse_stats(Name),
    ok.

%% @doc Increment `Name's `Counter' spiral.
-spec increment(Name :: atom(), Counter :: ok | blown | melt) -> ok.
increment(Name, Counter) ->
    _ = yz_stat_worker:update({update_fuse_stat, Name, Counter}),
    ok.