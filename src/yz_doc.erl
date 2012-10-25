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

%% @doc Functionality for working with Yokozuna documents.

%%%===================================================================
%%% API
%%%===================================================================

add_to_doc({doc, Fields}, Field) ->
    {doc, [Field|Fields]}.

-spec doc_id(riak_object:riak_object(), binary()) -> binary().
doc_id(O, Partition) ->
    <<(riak_object:key(O))/binary,"_",Partition/binary>>.

%% @doc Given an object generate the doc to be indexed by Solr.
-spec make_doc(riak_object:riak_object(), binary(), binary()) -> doc().
make_doc(O, FPN, Partition) ->
    ExtractedFields = extract_fields(O),
    Tags = extract_tags(O),
    Fields = [{id, doc_id(O, Partition)},
              {?YZ_ED_FIELD, gen_vc(O)},
              {?YZ_FPN_FIELD, FPN},
              {?YZ_NODE_FIELD, ?ATOM_TO_BIN(node())},
              {?YZ_PN_FIELD, Partition},
              {?YZ_RK_FIELD, riak_key(O)}],
    {doc, lists:append([Tags, ExtractedFields, Fields])}.

-spec extract_fields(obj()) ->  fields() | {error, any()}.
extract_fields(O) ->
    case yz_kv:is_tombstone(O) of
        false ->
            CT = yz_kv:get_obj_ct(O),
            Value = hd(riak_object:get_values(O)),
            ExtractorDef = yz_extractor:get_def(CT, [check_default]),
            case yz_extractor:run(Value, ExtractorDef) of
                {error, Reason} ->
                    ?ERROR("failed to index with reason ~s~nValue: ~s", [Reason, Value]),
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
-spec extract_tags(obj()) -> fields().
extract_tags(O) ->
    MD = yz_kv:metadata(O),
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

%% TODO: Just pass metadata in?
%%
%% TODO: I don't like having X-Riak-Last-Modified in here.  Add
%%       function to riak_object.
doc_ts(O) ->
    MD = riak_object:get_metadata(O),
    dict:fetch(<<"X-Riak-Last-Modified">>, MD).

doc_vclock(O) ->
    riak_object:vclock(O).

gen_ts() ->
    {{Year, Month, Day},
     {Hour, Min, Sec}} = calendar:now_to_universal_time(erlang:now()),
    list_to_binary(io_lib:format("~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0B",
                                 [Year,Month,Day,Hour,Min,Sec])).

gen_vc(O) ->
    TS = gen_ts(),
    RiakKey = riak_key(O),
    VClock = base64:encode(crypto:sha(term_to_binary(doc_vclock(O)))),
    <<TS/binary," ",RiakKey/binary," ",VClock/binary>>.

riak_key(O) ->
    riak_object:key(O).

value(O) ->
    riak_object:get_value(O).
