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
-define(YZ_ID_SEP, <<"*">>).
-define(YZ_ID_VER, <<"1">>).
-define(YZ_ED_VER, <<"2">>).

%%%===================================================================
%%% API
%%%===================================================================

add_to_doc({doc, Fields}, Field) ->
    {doc, [Field|Fields]}.

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

% @doc `true' if this Object has multiple contents
has_siblings(O) -> riak_object:value_count(O) > 1.

%% @doc Given an object generate the doc to be indexed by Solr.
-spec make_docs(obj(), hash(), hash(), binary(), binary()) -> [doc()].
make_docs(O, Hash, SchemaHash, FPN, Partition) ->
    [make_doc(O, Hash, SchemaHash, Content, FPN, Partition)
     || Content <- riak_object:get_contents(O)].

-spec make_doc(obj(), hash(), hash(), {dict(), dict()}, binary(), binary()) -> doc().
make_doc(O, Hash, SchemaHash, {MD, V}, FPN, Partition) ->
    Vtag = get_vtag(O, MD),
    DocId = doc_id(O, Partition, Vtag),
    EntropyData = gen_ed(O, Hash, SchemaHash, Partition),
    Bkey = {yz_kv:get_obj_bucket(O), yz_kv:get_obj_key(O)},
    Fields = make_fields({DocId, Bkey, FPN,
                          Partition, Vtag, EntropyData}),
    ExtractedFields = extract_fields({MD, V}),
    Tags = extract_tags(MD),
    {doc, lists:append([Tags, ExtractedFields, Fields])}.

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

make_fields({DocId, {Bucket, Key}, FPN, Partition, none, EntropyData}) ->
    [{?YZ_ID_FIELD, DocId},
     {?YZ_ED_FIELD, EntropyData},
     {?YZ_FPN_FIELD, FPN},
     {?YZ_PN_FIELD, Partition},
     {?YZ_RK_FIELD, Key},
     {?YZ_RT_FIELD, yz_kv:bucket_type(Bucket)},
     {?YZ_RB_FIELD, yz_kv:bucket_name(Bucket)}];

make_fields({DocId, BKey, FPN, Partition, Vtag, EntropyData}) ->
    Fields = make_fields({DocId, BKey, FPN, Partition, none, EntropyData}),
    [{?YZ_VTAG_FIELD, Vtag}|Fields].

%% @doc If this is a sibling, return its binary vtag
get_vtag(O, MD) ->
    case has_siblings(O) of
        true -> list_to_binary(yz_kv:get_md_entry(MD, ?MD_VTAG));
        _ -> none
    end.

-spec extract_fields({obj_metadata(), term()}) ->  fields() | {error, any()}.
extract_fields({MD, V}) ->
    case yz_kv:is_tombstone(MD) of
        false ->
            CT = yz_kv:get_obj_ct(MD),
            ExtractorDef = yz_extractor:get_def(CT, [check_default]),
            try
                case yz_extractor:run(V, ExtractorDef) of
                    {error, Reason} ->
                        yz_stat:index_fail(),
                        ?ERROR("failed to index fields from value with reason ~s~nValue: ~s", [Reason, V]),
                        [{?YZ_ERR_FIELD_S, 1}];
                    Fields ->
                        Fields
                end
            catch _:Err ->
                    yz_stat:index_fail(),
                    Trace = erlang:get_stacktrace(),
                    ?ERROR("failed to index fields from value with reason ~s ~p~nValue: ~s", [Err, Trace, V]),
                    [{?YZ_ERR_FIELD_S, 1}]
            end;
        true ->
            []
    end.


%% @private
%%
%% @doc Extract tags from object metadata.
-spec extract_tags(dict()) -> fields().
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
-spec get_user_meta(dict()) -> dict().
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

-spec get_tag(dict()) -> function().
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


%% @private
%%
%% @doc Get the tags names.
-spec get_tag_names(dict()) -> list().
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
%%       iterate.  Otherwise the doc would have to be fetched for each
%%       entry.
%%       Values are seperated by `*', like solr ids, since it's a rarely
%%       used character for bucket-types/names and keys.
gen_ed(O, Hash, SchemaHash, Partition) ->
    RiakBucket = yz_kv:get_obj_bucket(O),
    RiakBType = encode_doc_part(yz_kv:bucket_type(RiakBucket)),
    RiakBName = encode_doc_part(yz_kv:bucket_name(RiakBucket)),
    RiakKey = encode_doc_part(yz_kv:get_obj_key(O)),
    Hash64 = base64:encode(Hash),
    IOEd = [?YZ_ED_VER,?YZ_ID_SEP,Partition,?YZ_ID_SEP,SchemaHash,?YZ_ID_SEP,RiakBType,?YZ_ID_SEP,RiakBName,?YZ_ID_SEP,RiakKey,?YZ_ID_SEP,Hash64],
    iolist_to_binary(IOEd).


%% Meta keys and values can be strings or binaries
format_meta(key, Value) when is_binary(Value) ->
    format_meta(key, binary_to_list(Value));
format_meta(key, Value) ->
    list_to_binary(string:to_lower(Value));
format_meta(value, Value) when is_binary(Value) ->
    Value;
format_meta(value, Value) ->
    list_to_binary(Value).
