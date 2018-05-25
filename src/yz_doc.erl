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

%% @doc Functionality for working with Yokozuna documents.

-module(yz_doc).
-compile(export_all).
-include("yokozuna.hrl").

-define(MD_VTAG, <<"X-Riak-VTag">>).
-define(YZ_ID_SEP, "*").
-define(YZ_ID_VER, "1").
-define(YZ_ED_VER, <<"3">>).

-ifdef(namespaced_types).
-type riak_object_dict() :: dict:dict().
-else.
-type riak_object_dict() :: dict().
-endif.

%%%===================================================================
%%% API
%%%===================================================================

add_to_doc({doc, Fields}, Field) ->
    {doc, [Field|Fields]}.

%% @doc Get all doc ids for an object, possibly including docids for siblings
-spec doc_ids(obj(), binary()) -> [binary()].
doc_ids(RObj, Partition) ->
    Contents = riak_object:get_contents(RObj),
    SiblingVtags = sibling_vtags(Contents),
    [yz_doc:doc_id(RObj, Partition, Vtag) || Vtag <- SiblingVtags].

-spec doc_id(obj(), binary()) -> binary().
doc_id(O, Partition) ->
    doc_id(O, Partition, none).

-spec doc_id(obj(), binary(), term()) -> binary().
doc_id(O, Partition, Sibling) ->
    Bucket = yz_kv:get_obj_bucket(O),
    BType = encode_doc_part(yz_kv:bucket_type(Bucket)),
    BName = encode_doc_part(yz_kv:bucket_name(Bucket)),
    Key = encode_doc_part(yz_kv:get_obj_key(O)),
    IODocId = [?YZ_ID_VER,?YZ_ID_SEP,BType,?YZ_ID_SEP,BName,?YZ_ID_SEP,Key,?YZ_ID_SEP,Partition],
    case Sibling of
        none ->
            iolist_to_binary(IODocId);
        _ ->
            iolist_to_binary([IODocId,?YZ_ID_SEP,Sibling])
    end.

%% @doc grab all siblings' vtags from Object contents
sibling_vtags(Cs) when length(Cs) == 1 ->
    [none];
sibling_vtags(Cs) -> [get_vtag(MD) || {MD, _V} <- Cs].

%% @doc count of Object contents that are siblings and not tombstones
-spec live_siblings([{riak_object_dict(), riak_object:value()}]) -> non_neg_integer().
live_siblings(Cs) -> length([1 || {MD, _V} <- Cs, not yz_kv:is_tombstone(MD)]).

%% @doc Given an object generate the doc to be indexed by Solr.
-spec make_docs(obj(), hash(), binary(), binary()) -> [doc()].
make_docs(O, Hash, FPN, Partition) ->
    Contents = riak_object:get_contents(O),
    LiveSiblings = live_siblings(Contents),
    [make_doc(O, Hash, LiveSiblings, Content, FPN, Partition)
     || Content <- Contents].

-spec make_doc(obj(), hash(), non_neg_integer(), {yz_dict(), yz_dict()}, binary(),
               binary()) -> doc().
make_doc(O, Hash, LiveSiblings, {MD, V}, FPN, Partition) ->
    case yz_kv:is_tombstone(MD) of
        false ->
            Vtag = get_vtag(MD, LiveSiblings),
            DocId = doc_id(O, Partition, Vtag),
            EntropyData = gen_ed(O, Hash, Partition),
            Bkey = {yz_kv:get_obj_bucket(O), yz_kv:get_obj_key(O)},
            Fields = make_fields({DocId, Bkey, FPN,
                                  Partition, Vtag, EntropyData, Hash}),
            ExtractedFields = extract_fields({MD, V}),
            Tags = extract_tags(MD),
            {doc, lists:append([Tags, ExtractedFields, Fields])};
        true ->
            {doc, [{tombstone, ?TOMBSTONE}]}
    end.

%% @private
%% @doc encode `*' as %1, and `%' as %2. This is so we can reasonably use
%% the char `*' as a seperator for _yz_id parts (TBK+partition[+sibling]).
%% This removes the potential case where `*' included in a part can break
%% _yz_id uniqueness
-spec encode_doc_part(binary()) -> list().
encode_doc_part(Part) ->
    encode_doc_part(binary_to_list(Part), []).

encode_doc_part([], Acc) ->
  lists:reverse(Acc);
encode_doc_part([$* | Rest], Acc) ->
  encode_doc_part(Rest, ["%1" | Acc]);
encode_doc_part([$% | Rest], Acc) ->
  encode_doc_part(Rest, ["%2" | Acc]);
encode_doc_part([C | Rest], Acc) ->
  encode_doc_part(Rest, [C | Acc]).

make_fields({DocId, {Bucket, Key}, FPN, Partition, none, EntropyData, Hash}) ->
    [{?YZ_ID_FIELD, DocId},
     {?YZ_ED_FIELD, EntropyData}, %% deprecated
     {?YZ_HA_FIELD, base64:encode(Hash)},
     {?YZ_FPN_FIELD, FPN},
     {?YZ_PN_FIELD, Partition},
     {?YZ_RK_FIELD, Key},
     {?YZ_RT_FIELD, yz_kv:bucket_type(Bucket)},
     {?YZ_RB_FIELD, yz_kv:bucket_name(Bucket)}];

make_fields({DocId, BKey, FPN, Partition, Vtag, EntropyData, Hash}) ->
    Fields = make_fields({DocId, BKey, FPN, Partition, none, EntropyData, Hash}),
    [{?YZ_VTAG_FIELD, Vtag}|Fields].

%% @doc Get vtag for MetaData entry.
get_vtag(MD) ->
    case yz_kv:get_md_entry(MD, ?MD_VTAG) of
        none -> none;
        Val -> list_to_binary(Val)
    end.

%% @doc If this is a sibling and not a tombstone val, return its binary vtag
get_vtag(MD, LiveSiblings) ->
    case LiveSiblings > 1 of
        true -> get_vtag(MD);
        _ -> none
    end.

-spec extract_fields({obj_metadata(), term()}) -> fields() | [{error, any()}].
extract_fields({MD, V}) ->
    try
        CT = yz_kv:get_obj_ct(MD),
        ExtractorDef = yz_extractor:get_def(CT, [check_default]),
        case yz_extractor:run(V, ExtractorDef) of
            {error, Reason} ->
                yz_stat:index_extract_fail(),
                ?ERROR("Failed to extract fields from value with reason ~p.  Value: ~p", [Reason, V]),
                [{?YZ_ERR_FIELD_S, 1}];
            Fields ->
                Fields
        end
    catch _:Err ->
            yz_stat:index_extract_fail(),
            ?ERROR("An exception occurred extracting fields from value with reason ~p. Value: ~p", [Err, V]),
            [{?YZ_ERR_FIELD_S, 1}]
    end.

%% @private
%%
%% @doc Extract tags from object metadata.
-spec extract_tags(yz_dict()) -> fields().
extract_tags(MD) ->
    MD2 = get_user_meta(MD),
    TagNames = get_tag_names(MD2),
    lists:foldl(get_tag(MD2), [], TagNames).

%% @private
%%
%% @doc Get the user metdata from the Riak Object metadata.
%%
%% NOTE: This function should return the same type as the top-level
%%       Riak Object metadata so that `yz_kv:get_md_entry' may be
%%       used.  This way when KV is altered to store user meta at the
%%       top-level the migration will be easier.
-spec get_user_meta(yz_dict()) -> yz_dict().
get_user_meta(MD) ->
    case yz_kv:get_md_entry(MD, <<"X-Riak-Meta">>) of
        none ->
            dict:new();
        MetaMeta ->

            %% NOTE: Need to call `to_lower' because
            %%       `erlang:decode_packet' which is used by mochiweb
            %%       will modify the case of certain header names
            MM2 = [{format_meta(key, K), format_meta(value, V)}
                   || {K,V} <- MetaMeta],
            dict:from_list(MM2)
    end.

-spec get_tag(yz_dict()) -> function().
get_tag(MD) ->
    fun(TagName, Fields) ->
            case yz_kv:get_md_entry(MD, TagName) of
                none ->
                    Fields;
                Value ->
                    case strip_prefix(TagName) of
                        ignore -> Fields;
                        TagName2 -> [{TagName2, Value}|Fields]
                    end
            end
    end.

-spec strip_prefix(binary()) -> binary() | ignore.
strip_prefix(<<"x-riak-meta-",Tag/binary>>) ->
    Tag;
strip_prefix(_) ->
    %% bad tag, silently discard
    ignore.

-spec adding_docs([doc()]) -> [doc()].
adding_docs(Docs) ->
    [{doc, Fields} || {doc, Fields} <- Docs,
                     not proplists:is_defined(tombstone, Fields)].

%% @private
%%
%% @doc Get the tags names.
-spec get_tag_names(yz_dict()) -> list().
get_tag_names(MD) ->
    case yz_kv:get_md_entry(MD, <<"x-riak-meta-yz-tags">>) of
        none -> [];
        TagNames -> split_tag_names(TagNames)
    end.

%% @private
%%
%% @doc Split the tag names.  Input is assumed to be CSV.  Whitespace
%%      is stripped.  Tag names are converted to lower-case.
-spec split_tag_names(binary()) -> [binary()].
split_tag_names(TagNames) ->
    NoSpace = binary:replace(TagNames, <<" ">>, <<"">>, [global]),
    Lower = list_to_binary(string:to_lower(binary_to_list(NoSpace))),
    binary:split(Lower, <<",">>, [global]).

%%%===================================================================
%%% Private
%%%===================================================================

%% NOTE: All of this data needs to be in one field to efficiently
%%       iterate. Otherwise the doc would have to be fetched for each entry.
gen_ed(O, Hash, Partition) ->
    RiakBucket = yz_kv:get_obj_bucket(O),
    RiakBType = base64:encode(yz_kv:bucket_type(RiakBucket)),
    RiakBName = base64:encode(yz_kv:bucket_name(RiakBucket)),
    RiakKey = base64:encode(yz_kv:get_obj_key(O)),
    Hash64 = base64:encode(Hash),
    <<?YZ_ED_VER/binary," ",Partition/binary," ",RiakBType/binary," ",
      RiakBName/binary," ",RiakKey/binary," ",Hash64/binary>>.


%% Meta keys and values can be strings or binaries
format_meta(key, Value) when is_binary(Value) ->
    format_meta(key, binary_to_list(Value));
format_meta(key, Value) ->
    list_to_binary(string:to_lower(Value));
format_meta(value, Value) when is_binary(Value) ->
    Value;
format_meta(value, Value) ->
    list_to_binary(Value).
