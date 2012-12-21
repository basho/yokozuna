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

%%%===================================================================
%%% Records
%%%===================================================================

-record(yz_index_cmd, {
          doc :: doc(),
          index :: string(),
          req_id :: non_neg_integer()
         }).

-record(yz_search_cmd, {
          qry :: term(),
          req_id :: non_neg_integer()
         }).

%% A reference to a merkle tree.
-record(tree_ref, {
          index :: string(),
          name :: tree_name(),
          pid :: pid(),
          ref :: reference()
         }).

%%%===================================================================
%%% Types
%%%===================================================================

%% Shorthand for existing types
-type datetime() :: calendar:datetime().
-type orddict(K,V) :: [{K,V}].
-type ordset(T) :: ordsets:ordset(T).
-type proplist() :: proplists:proplist().
-type ring() :: riak_core_ring:riak_core_ring().
-type timestamp() :: erlang:timestamp().

-type index_set() :: ordset(index_name()).
-type base64() :: binary().

%% An iso8601 datetime as binary, e.g. <<"20121221T000000">>.
-type iso8601() :: binary().
-type tree_name() :: atom().
-type tree_ref() :: #tree_ref{}.

%% N value
-type n() :: pos_integer().
%% Number of partitions
-type q() :: pos_integer().
%% Partition
-type p() :: non_neg_integer().
%% Logical Partition
-type lp() :: pos_integer().
%% Distance between LPs
-type dist() :: non_neg_integer().
%% Mapping from logical partition to partition
-type logical_idx() :: [{lp(), p()}].
-type logical_filter() :: all | [lp()].
-type filter() :: all | [p()].
-type p_node() :: {p(), node()}.
-type lp_node() :: {lp(), node()}.
-type cover_set() :: [p_node()].
-type logical_cover_set() :: [lp_node()].
-type filter_cover_set() :: [{p_node(), filter()}].

-type ring_event() :: {ring_event, riak_core_ring:riak_core_ring()}.
-type event() :: ring_event().


%%%===================================================================
%%% Macros
%%%===================================================================

-define(ATOM_TO_BIN(A), list_to_binary(atom_to_list(A))).
-define(BIN_TO_INT(B), list_to_integer(binary_to_list(B))).
-define(INT_TO_BIN(I), list_to_binary(integer_to_list(I))).
-define(INT_TO_STR(I), integer_to_list(I)).
-define(PARTITION_BINARY(S), S#state.partition_binary).
-define(HEAD_CTYPE, "content-type").

-define(DATA_DIR, application:get_env(riak_core, platform_data_dir)).

-define(YZ_DEFAULT_SOLR_PORT, "8983").
-define(YZ_DEFAULT_SOLR_STARTUP_WAIT, 15).
-define(YZ_DEFAULT_TICK_INTERVAL, 60000).
-define(YZ_EVENTS_TAB, yz_events_tab).
-define(YZ_ROOT_DIR, app_helper:get_env(?YZ_APP_NAME, root_dir, "data/yz")).
-define(YZ_PRIV, code:priv_dir(?YZ_APP_NAME)).
-define(YZ_CORE_CFG_FILE, "config.xml").
-define(YZ_INDEX_CMD, #yz_index_cmd).
-define(YZ_SEARCH_CMD, #yz_search_cmd).
-define(YZ_APP_NAME, yokozuna).
-define(YZ_SVC_NAME, yokozuna).
-define(YZ_VNODE_MASTER, yokozuna_vnode_master).
-define(YZ_META_INDEXES, yokozuna_indexes).

-define(YZ_ERR_NOT_ENOUGH_NODES,
        "Not enough nodes are up to service this request.").

%%%===================================================================
%%% Anti Entropy
%%%===================================================================

-define(YZ_AE_DIR,
        application:get_env(?YZ_APP_NAME, anti_entropy_data_dir)).
-define(YZ_ENTROPY_TICK,
        app_helper:get_env(?YZ_APP_NAME, entropy_tick, 60000)).

-type hashtree() :: hashtree:hashtree().
-type exchange() :: {p(), {p(), n()}}.
-type exchange_mode() :: automatic | manual.
-type tree() :: pid().
-type trees() :: orddict(p(), tree()).
-type ed_filter() :: [{before, iso8601()} |
                      {continuation, ed_continuation()} |
                      {partition, lp()} |
                      {limit, pos_integer()}].
-type ed_continuation() :: none | base64().

-record(entropy_data, {
          more=false :: boolean(),
          continuation :: ed_continuation(),
          pairs :: [{DocID::binary(), Hash::base64()}]
         }).
-type entropy_data() :: #entropy_data{}.


%%%===================================================================
%%% Riak KV
%%%===================================================================

-type obj() :: riak_object:riak_object().
-type obj_metadata() :: dict().

%%%===================================================================
%%% Docs
%%%===================================================================

-type field_name() :: atom() | binary().
-type field_value() :: binary().
-type field() :: {field_name(), field_value()}.
-type fields() :: [field()].
-type doc() :: {doc, fields()}.

%%%===================================================================
%%% Extractors
%%%===================================================================

-type mime_type() :: binary() | string() | default.
-type extractor_name() :: atom().
-type extractor_def() :: extractor_name() | {extractor_name(), proplist()}.
-type extractor_map() :: orddict(mime_type(), extractor_def()).

-define(NO_OPTIONS, []).

%%%===================================================================
%%% Logging
%%%===================================================================

-define(DEBUG(Fmt, Args), lager:debug(Fmt, Args)).
-define(ERROR(Fmt, Args), lager:error(Fmt, Args)).
-define(INFO(Fmt, Args), lager:info(Fmt, Args)).
-define(WARN(Fmt, Args), lager:warning(Fmt, Args)).

%%%===================================================================
%%% Indexes
%%%===================================================================

-record(index_info,
        {
          name :: index_name(),
          schema_name :: schema_name()
        }).

-type indexes() :: orddict(index_name(), index_info()).
-type index_info() :: #index_info{}.
-type index_name() :: string().

-define(YZ_DEFAULT_INDEX, "_yz_default").
-define(YZ_INDEX_CONTENT, yz_index_content).

%%%===================================================================
%%% Schemas
%%%===================================================================

-define(YZ_DEFAULT_SCHEMA_FILE,
        filename:join([?YZ_PRIV, "default_schema.xml"])).
-define(YZ_DEFAULT_SCHEMA_NAME, <<"_yz_default">>).
-define(YZ_SCHEMA_BUCKET, <<"_yz_schema">>).

-type raw_schema() :: binary().
-type schema_name() :: binary().

%%%===================================================================
%%% Solr Fields
%%%===================================================================

%% Entropy Data
-define(YZ_ED_FIELD, '_yz_ed').

%% First Partition Number
-define(YZ_FPN_FIELD, '_yz_fpn').
-define(YZ_FPN_FIELD_S, "_yz_fpn").
-define(YZ_FPN_FIELD_B, <<"_yz_fpn">>).

%% Sibling VTags
-define(YZ_VTAG_FIELD, '_yz_vtag').
-define(YZ_VTAG_FIELD_S, "_yz_vtag").
-define(YZ_VTAG_FIELD_B, <<"_yz_vtag">>).

%% Node
-define(YZ_NODE_FIELD, '_yz_node').
-define(YZ_NODE_FIELD_S, "_yz_node").

%% Partition Number
-define(YZ_PN_FIELD, '_yz_pn').
-define(YZ_PN_FIELD_S, "_yz_pn").
-define(YZ_PN_FIELD_B, <<"_yz_pn">>).

%% Riak key
-define(YZ_RK_FIELD, '_yz_rk').
-define(YZ_RK_FIELD_S, "_yz_rk").
-define(YZ_RK_FIELD_B, <<"_yz_rk">>).
