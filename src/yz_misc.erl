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
-include("yokozuna.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Add list of webmachine routes to the router.
add_routes(Routes) ->
    [webmachine_router:add_route(R) || R <- Routes].

%% @doc Recursively copy each file to `Dir'.
-spec copy_files([string()], string()) -> ok.
copy_files([], _) ->
    ok;
copy_files([File|Rest], Dir) ->
    Basename = filename:basename(File),
    case filelib:is_dir(File) of
        true ->
            NewDir = filename:join([Dir, Basename]),
            yz_misc:make_dir(NewDir),
            copy_files(filelib:wildcard(filename:join([File, "*"])), NewDir),
            copy_files(Rest, Dir);
        false ->
            {ok, _} = file:copy(File, filename:join([Dir, Basename])),
            copy_files(Rest, Dir)
    end.

%% @doc Calculate the delta between `Old' and `New'.
-spec delta(ordset(any()), ordset(any())) ->
                   {ordset(any()), ordset(any()), ordset(any())}.
delta(Old, New) ->
    Removed = ordsets:subtract(Old, New),
    Added = ordsets:subtract(New, Old),
    Same = ordsets:intersection(New, Old),
    {Removed, Added, Same}.

%% @doc Attempt to get the `Key' from `Dict'.  If it doesn't exist
%%      then return `Default'.
-spec dict_get(term(), dict(), term()) -> term().
dict_get(Key, Dict, Default) ->
    case dict:find(Key, Dict) of
        {ok, Val} -> Val;
        error -> Default
    end.

%% @doc Get either the `raw' or `transformed' ring.  The raw ring is
%%      what is stored on disk.  The transformed ring is the raw ring
%%      with processing done to it such as bucket fixups.
-spec get_ring(raw | transformed) -> ring().
get_ring(raw) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Ring;
get_ring(transformed) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Ring.

%% @doc Create the `Dir' if it doesn't already exist.
-spec make_dir(string()) -> ok.
make_dir(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            ok;
        false ->
            ok = filelib:ensure_dir(Dir),
            ok = file:make_dir(Dir)
    end.

%% @doc Create each dir in the list.
-spec make_dirs([string()]) -> ok.
make_dirs([]) ->
    ok;
make_dirs([Dir|Rest]) ->
    ok = make_dir(Dir),
    make_dirs(Rest).

-spec delete_dir(string() | atom() | binary()) -> ok | {error, atom()}.
delete_dir(Dir) ->
    case filelib:is_file(Dir) of
        true ->
            case filelib:is_dir(Dir) of
                true -> delete_dir([Dir], []);
                false -> {error, enotdir}
            end;
        false -> {error, enoent}
    end.

delete_dir([], Acc) -> Acc;
delete_dir([Path|Paths], Acc) ->
    delete_dir(Paths,
        case filelib:is_dir(Path) of
            false ->
              file:delete(Path);
              % ok;
            true ->
                {ok, Listing} = file:list_dir(Path),
                SubPaths = [filename:join(Path, Name) || Name <- Listing],
                delete_dir(SubPaths, [Path | Acc]),
                % dir should be clean by now
                file:del_dir(Path)
                % ok
        end).

%% @doc Take a list `L' and pair adjacent elements wrapping around the
%%      end by pairing the first with the last.
-spec make_pairs([T]) -> [{T,T}].
make_pairs(L) ->
    make_pairs(L, hd(L), []).

%% @doc Return the list of partitions owned and about to be owned by
%%      this `Node' for the given `Ring'.
-spec owned_and_next_partitions(node(), riak_core_ring:riak_core_ring()) ->
                                       ordset(p()).
owned_and_next_partitions(Node, Ring) ->
    Owned = lists:filter(is_owner(Node), riak_core_ring:all_owners(Ring)),
    Next = lists:filter(is_owner(Node), riak_core_ring:all_next_owners(Ring)),
    ordsets:from_list([P || {P,_} <- Next ++ Owned]).

%% @doc Set the ring metadata for the given `Name' to `Value'
-spec set_ring_meta(atom(), any()) -> ring().
set_ring_meta(Name, Value) ->
    Ring = get_ring(raw),
    Ring2 = riak_core_ring:update_meta(Name, Value, Ring),
    F = set_ring_trans(Ring2),
    {ok, Ring3} = riak_core_ring_manager:ring_trans(F, none),
    Ring3.

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
is_owner(Node) ->
    fun({_P, Owner}) -> Node == Owner end.

%% @private
make_pairs([Last], First, Pairs) ->
    [{Last, First}|lists:reverse(Pairs)];
make_pairs([A,B|T], _First, Pairs) ->
    make_pairs([B|T], _First, [{A,B}|Pairs]).

%% @private
set_ring_trans(Ring) ->
    fun(_,_) -> {new_ring, Ring} end.
