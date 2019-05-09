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
-compile([export_all, nowarn_export_all]).
-include("yokozuna.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("kernel/include/file.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Extract the hostname from `Node'.
-spec hostname(node()) -> string().
hostname(Node) ->
    S = atom_to_list(Node),
    [_, Host] = re:split(S, "@", [{return, list}]),
    Host.

%% @doc Add list of webmachine routes to the router.
add_routes(Routes) ->
    [webmachine_router:add_route(R) || R <- Routes].

-spec convert_preflist(list(), logical | real) -> list().
convert_preflist(Preflist, logical) ->
    LI = yz_cover:logical_index(yz_misc:get_ring(transformed)),
    [convert_pl_entry(Entry, logical, LI) || Entry <- Preflist].

-spec convert_pl_entry(term(), logical | real, logical_idx()) -> term().
convert_pl_entry({Partition,Node}, logical, Map) ->
    LPartition = yz_cover:logical_partition(Map, Partition),
    {LPartition,Node}.

%% @doc Recursively copy each file to `Dir'.
%% Defaults to `overwrite' existing files. You may also
%% set to only `update' with more recent files,
%% which first compares timestamps. Finally, you can
%% `skip' any duplicate files.
-spec copy_files([string()], string(), overwrite | update | skip) -> ok.
copy_files([], _, _) ->
    ok;
copy_files([File|Rest], Dir, Action) ->
    Basename = filename:basename(File),
    case filelib:is_dir(File) of
        true ->
            NewDir = filename:join([Dir, Basename]),
            yz_misc:make_dir(NewDir),
            copy_files(filelib:wildcard(filename:join([File, "*"])), NewDir),
            copy_files(Rest, Dir);
        false ->
            DestFile = filename:join([Dir, Basename]),
            case should_copy(Action, File, DestFile) of
                true ->
                    {ok, _} = file:copy(File, DestFile);
                false -> noop
            end,
            copy_files(Rest, Dir)
    end.

%% @doc Recursively copy each file to `Dir',
%% overwriting any exiting files
-spec copy_files([string()], string()) -> ok.
copy_files([], _) ->
    ok;
copy_files([File|Rest], Dir) ->
    copy_files([File|Rest], Dir, overwrite).

%% If destination exists, skip the copy
should_copy(skip, _Source, Destination) ->
    case file:read_file_info(Destination) of
        {error, enoent} -> true;
        {ok, _}    -> false;
        {error, _} -> false
    end;
%% If source timestamp is newer than destination, or
%% if destination does not exist, perform the copy
should_copy(update, Source, Destination) ->
    case file:read_file_info(Destination) of
        {error, enoent} -> true;
        {ok, DestFileInfo} ->
            case file:read_file_info(Source) of
                {ok, SourceFileInfo} ->
                    SourceFileInfo#file_info.mtime > DestFileInfo#file_info.mtime;
                {error, _} -> false
            end;
        {error, _} -> false
    end;
should_copy(overwrite, _Source, _Destination) ->
    true.


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
-spec dict_get(term(), yz_dict(), term()) -> term().
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

%% @doc Group a list of tuples by common key.  Builds a list of all
%%      values associated with the same key.  Returns a list in
%%      `orddict()' format.
%%
%%   `PL' - List of tuples of form `[{Key,Value}]'.
%%
%%   `Transform' - Function which takes `{Key,Value}' and transforms
%%                 into a potentially different `{Key,Value}'.
-type xform() :: fun(({term(),term()}) -> {term(),term()}).
-spec group_by(proplist(), xform()) -> od().
group_by(PL, Transform) ->
    lists:foldl(group_by_1(Transform), orddict:new(), PL).

%% @private
-spec group_by_1(xform()) -> fun(({term(),term()}, od()) -> od()).
group_by_1(Transform) ->
    fun(Elem, Acc) ->
            {K,V} = Transform(Elem),
            Cons = fun(CurrentVal) -> [V|CurrentVal] end,
            orddict:update(K, Cons, [V], Acc)
    end.

%% @doc Determine the owner of the index (riak partition).
-spec index_owner(ring(), p()) -> node() | no_owner.
index_owner(Ring, Index) ->
    try
        riak_core_ring:index_owner(Ring, Index)
    catch error:{badmatch,_} ->
            no_owner
    end.

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

%% @doc Filter out all entries for partitions that are not currently owned or
%%      this node is a future owner of.
-spec filter_out_fallbacks(ordset(p), solr_entries()) -> [{bkey(), object_pair(),
    write_reason(), p()}].
filter_out_fallbacks(OwnedAndNext, Entries) ->
    lists:filter(fun({_Bkey, _Obj, _Reason, P}) ->
        ordsets:is_element(P, OwnedAndNext)
    end, Entries).


%% @doc Calculate the primary preflist for a given Bucket/Key and N
%%      value.  It is a "primary" preflist because it assumes all nodes
%%      in the cluster are reachable.
-spec primary_preflist(bkey(), ring(), n()) -> preflist().
primary_preflist(BKey, Ring, N) ->
    Idx = riak_core_util:chash_key(BKey),
    lists:sublist(riak_core_ring:preflist(Idx, Ring), N).

-spec queue_pop(list(), function()) ->
                       {Items::any(), NewQueue::list()} | empty.
queue_pop([], Refill) ->
    case Refill() of
        [] -> empty;
        Items -> queue_pop(Items, Refill)
    end;
queue_pop(Queue, _Refill) ->
    [Item|Rest] = Queue,
    {Item,Rest}.

%% @doc Set the ring metadata for the given `Name' to the value
%% returned by applying `Fun' to the existing value and `Arg'.  The
%% `Fun' is evaluated as `Fun(OldValue, Arg)' inside of a `ring_trans'
%% operation. The `Fun' should return `NewValue' to be associated with
%% `Name'. If `NewValue == OldValue', the ring modification is ignored.
-spec set_ring_meta(atom(), any(), fun((any(), any()) -> any()), any())
         -> {ok, ring()} | not_changed.
set_ring_meta(Name, Default, Fun, Arg) ->
    riak_core_ring_manager:ring_trans(
      fun set_ring_trans/2,
      {Name, Default, Fun, Arg}).

-spec compress(iodata()) -> iolist().
compress(Data) ->
    Z = zlib:open(),
    ok = zlib:deflateInit(Z),
    Compressed = zlib:deflate(Z, Data, finish),
    ok = zlib:deflateEnd(Z),
    zlib:close(Z),
    Compressed.

-spec decompress(iodata()) -> iolist().
decompress(Data) ->
    Z = zlib:open(),
    zlib:inflateInit(Z),
    Decompressed = zlib:inflate(Z, Data),
    zlib:inflateEnd(Z),
    zlib:close(Z),
    Decompressed.

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
set_ring_trans(Ring, {Name, Default, Fun, Arg}) ->
    Old = case riak_core_ring:get_meta(Name, Ring) of
              {ok, O} -> O;
              undefined -> Default
          end,
    case Fun(Old, Arg) of
        Old ->
            ignore;
        New ->
            %% We sleep one second here because we need to be slower
            %% than the resolution of the lastmod field of
            %% riak_core_ring's meta_entry record; we can remove this
            %% sleep when we move index information out of the ring
            %%
            %% Unfortunately, this blocks riak_core_ring_manager, but
            %% only for ring changes, since all ring reads check an
            %% ETS table instead of hitting the manager gen_server.
            timer:sleep(1000),
            {new_ring, riak_core_ring:update_meta(Name, New, Ring)}
    end.
