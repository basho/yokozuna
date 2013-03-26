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

-module(yz_doc).
-compile(export_all).
-include("yokozuna.hrl").

-define(MD_VTAG, <<"X-Riak-VTag">>).

%% @doc Functionality for working with Yokozuna documents.

%%%===================================================================
%%% API
%%%===================================================================

add_to_doc({doc, Fields}, Field) ->
    {doc, [Field|Fields]}.

-spec doc_id(obj(), binary()) -> binary().
doc_id(O, Partition) ->
    <<(yz_kv:get_obj_key(O))/binary,"_",Partition/binary>>.

doc_id(O, Partition, none) ->
    doc_id(O, Partition);

doc_id(O, Partition, Sibling) ->
    <<(riak_object:key(O))/binary,"_",Partition/binary,"_",Sibling/binary>>.

% @doc `true' if this Object has multiple contents
has_siblings(O) -> riak_object:value_count(O) > 1.

%% @doc Given an object generate the doc to be indexed by Solr.
-spec make_docs(obj(), binary(), binary(), boolean()) -> [doc()].
make_docs(O, FPN, Partition, IndexContent) ->
    [make_doc(O, Content, FPN, Partition, IndexContent)
     || Content <- riak_object:get_contents(O)].

-spec make_doc(obj(), {dict(), dict()}, binary(), binary(), boolean()) -> doc().
make_doc(O, {MD, V}, FPN, Partition, IndexContent) ->
    Vtag = get_vtag(O, MD),
    DocId = doc_id(O, Partition, Vtag),
    EntropyData = gen_ed(O, Partition),
    Fields = make_fields({DocId, yz_kv:get_obj_key(O), FPN,
                          Partition, Vtag, EntropyData}),
    ExtractedFields =
        case IndexContent of
            true -> extract_fields({MD, V});
            false -> []
        end,
    Tags = extract_tags(MD),
    {doc, lists:append([Tags, ExtractedFields, Fields])}.

make_fields({DocId, Key, FPN, Partition, none, EntropyData}) ->
    [{?YZ_ID_FIELD, DocId},
     {?YZ_ED_FIELD, EntropyData},
     {?YZ_FPN_FIELD, FPN},
     {?YZ_NODE_FIELD, ?ATOM_TO_BIN(node())},
     {?YZ_PN_FIELD, Partition},
     {?YZ_RK_FIELD, Key}];

make_fields({DocId, Key, FPN, Partition, Vtag, EntropyData}) ->
    make_fields({DocId, Key, FPN, Partition, none, EntropyData}) ++
      [{?YZ_VTAG_FIELD, Vtag}].

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
            case yz_extractor:run(V, ExtractorDef) of
                {error, Reason} ->
                    ?ERROR("failed to index with reason ~s~nValue: ~s", [Reason, V]),
                    {error, Reason};
                Fields ->
                    Fields
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
            MM2 = [{list_to_binary(string:to_lower(K)), list_to_binary(V)}
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

%% TODO: I don't like having X-Riak-Last-Modified in here.  Add
%%       function to riak_object.
doc_ts(MD) ->
    dict:fetch(<<"X-Riak-Last-Modified">>, MD).

gen_ts() ->
    {{Year, Month, Day},
     {Hour, Min, Sec}} = calendar:now_to_universal_time(erlang:now()),
    list_to_binary(io_lib:format("~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0B",
                                 [Year,Month,Day,Hour,Min,Sec])).

%% NOTE: All of this data needs to be in one field to efficiently
%%       iterate.  Otherwise the doc would have to be fetched for each
%%       entry.
gen_ed(O, Partition) ->
    TS = gen_ts(),
    RiakBucket = yz_kv:get_obj_bucket(O),
    RiakKey = yz_kv:get_obj_key(O),
    %% TODO: do this in KV vnode and pass to hook
    Hash = base64:encode(yz_kv:hash_object(O)),
    <<TS/binary," ",Partition/binary," ",RiakBucket/binary," ",RiakKey/binary," ",Hash/binary>>.
