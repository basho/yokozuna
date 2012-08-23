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

-module(yz_misc).
-compile(export_all).
-include_lib("webmachine/include/webmachine.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Add list of webmachine routes to the router.
add_routes(Routes) ->
    [webmachine_router:add_route(R) || R <- Routes].

%% @doc Return the list of partitions owned and about to be owned by
%%      this `Node' for the given `Ring'.
-spec owned_and_next_partitions(node(), riak_core_ring:riak_core_ring()) ->
                                       ordsets:ordset().
owned_and_next_partitions(Node, Ring) ->
    Owned = lists:filter(is_owner(Node), riak_core_ring:all_owners(Ring)),
    Next = lists:filter(is_owner(Node), riak_core_ring:all_next_owners(Ring)),
    ordsets:from_list([P || {P,_} <- Next ++ Owned]).

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
is_owner(Node) ->
    fun({P,Owner}) -> Node == Owner end.
