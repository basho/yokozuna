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
%%% Includes
%%%===================================================================

-include_lib("xmerl/include/xmerl.hrl").

%%%===================================================================
%%% Types
%%%===================================================================

%% Shorthand for existing types
-type datetime() :: calendar:datetime().
-type od() :: orddict:orddict().
-type ordset(T) :: ordsets:ordset(T).
-type proplist() :: proplists:proplist().
-type ring() :: riak_core_ring:riak_core_ring().
-type timestamp() :: erlang:timestamp().
-type pipe() :: riak_pipe:pipe().

-type index_set() :: ordset(index_name()).
-type base64() :: binary().
-type hash() :: binary().
-type filename() :: string().

%% milliseconds
-type ms() :: non_neg_integer().
-type seconds() :: non_neg_integer().

-type predicate(A) :: fun((A) -> boolean()).

%% An iso8601 datetime as binary, e.g. <<"20121221T000000">>.
-type iso8601() :: binary().
-type tree_name() :: atom().

%% Index into the ring
-type idx() :: non_neg_integer().
%% N value
-type n() :: pos_integer().
%% Number of partitions
-type q() :: pos_integer().
%% Partition, the starting idx() of a partition
-type p() :: non_neg_integer().
%% Logical Partition
-type lp() :: pos_integer().
%% Preflist, list of partition/owner pairs
-type preflist() :: [{p(),term()}].
%% Short representation of a preflist, partition + n_val
-type short_preflist() :: {p(), n()}.
%% Riak bucket
-type bucket() :: Name :: binary() | {Type :: binary(), Name :: binary()}.
%% Riak key
-type key() :: binary().
%% Bucket/Key pair
-type bkey() :: {bucket(), key()}.
%% Distance between LPs
-type dist() :: non_neg_integer().
%% Mapping from logical partition to partition
-type logical_idx() :: [{lp(), p()}].
-type logical_filter() :: all | [lp()].
-type filter() :: all | [p()].
-type p_node() :: {p(), node()}.
-type lp_node() :: {lp(), node()}.
-type p_set() :: [p_node()].
-type logical_cover_pair() :: {lp_node(), logical_filter()}.
-type logical_cover_set() :: [logical_cover_pair()].
-type solr_host_mapping() :: [{node(), {string(),string()}}].
-type plan() :: {[node()], logical_cover_set(), solr_host_mapping()}.

-type ring_event() :: {ring_event, riak_core_ring:riak_core_ring()}.
-type event() :: ring_event().

%% @doc The `component()' type represents components that may be
%%      enabled or disabled at runtime.  Typically a component is
%%      disabled in a live, production cluster in order to isolate
%%      issues.
%%
%%  `search' - The ability to run queries.  This will only prevent
%%    query requests that arrive via HTTP or Protocol Buffers.  The
%%    operator will still be able to run searches via
%%    `yokozuna:search/3' to potentially aid in diagnosis of issues.
%%
%%  `index' - The ability to index Riak Objects as they are written.
%%    While disabled inconsistency will arise between KV and Yokozuna
%%    as data is written.  Either the operator will have to take
%%    action to manually index the missing data or wait for AAE to
%%    take care of it.
-type component() :: search | index.

%%%===================================================================
%%% Macros
%%%===================================================================

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).

-define(IF(Expression, Action),
        if Expression -> Action, ok;
           true -> ok
        end).
-define(ATOM_TO_BIN(A), list_to_binary(atom_to_list(A))).
-define(BIN_TO_ATOM(B), list_to_atom(binary_to_list(B))).
-define(BIN_TO_INT(B), list_to_integer(binary_to_list(B))).
-define(BIN_TO_FLOAT(B), list_to_float(binary_to_list(B))).
-define(INT_TO_BIN(I), list_to_binary(integer_to_list(I))).
-define(INT_TO_STR(I), integer_to_list(I)).
-define(INT_TO_ATOM(I), list_to_atom(integer_to_list(I))).
-define(FLOAT_TO_BIN(F), list_to_binary(float_to_list(F))).
-define(PARTITION_BINARY(S), S#state.partition_binary).
-define(HEAD_CTYPE, "content-type").
-define(YZ_HEAD_EXTRACTOR, "yz-extractor").

-define(DATA_DIR, application:get_env(riak_core, platform_data_dir)).

%% Default timeout for index (put) creation.
-define(DEFAULT_IDX_CREATE_TIMEOUT, 45000).

-define(MAYBE(Check, Expression, Default),
        case Check of
            true -> Expression;
            false -> Default
        end).

-define(MAYBE(Check, Expression),
        ?MAYBE(Check, Expression, ok)).

%% Core security requires `{BucketType, Bucket}'. Technically, these
%% arent so strict, and so we refer to them as `{Resource,
%% SubResource}' where `Resource' is a resource like index or schema
%% and `SubResource' is a specific instance.
-define(YZ_SECURITY_INDEX, <<"index">>).
-define(YZ_SECURITY_SCHEMA, <<"schema">>).
-define(YZ_SECURITY_SEARCH_PERM, "search.query").
-define(YZ_SECURITY_ADMIN_PERM, "search.admin").

-define(YZ_COVER_TICK_INTERVAL, app_helper:get_env(?YZ_APP_NAME, cover_tick, 2000)).
-define(YZ_DEFAULT_SOLR_PORT, 8983).
-define(YZ_DEFAULT_SOLR_STARTUP_WAIT, 30).
%% TODO: See if using mochiglobal for this makes difference in performance.
-define(YZ_ENABLED, app_helper:get_env(?YZ_APP_NAME, enabled, false)).
-define(YZ_ROOT_DIR, app_helper:get_env(?YZ_APP_NAME, root_dir,
                        app_helper:get_env(riak_core, platform_data_dir)++"/yz")).
-define(YZ_TEMP_DIR, app_helper:get_env(?YZ_APP_NAME, temp_dir,
                        app_helper:get_env(riak_core, platform_data_dir)++"/yz_temp")).
-define(YZ_PRIV, code:priv_dir(?YZ_APP_NAME)).
-define(YZ_CORE_CFG_FILE, "solrconfig.xml").
-define(YZ_INDEX_CMD, #yz_index_cmd).
-define(YZ_SEARCH_CMD, #yz_search_cmd).
-define(YZ_APP_NAME, yokozuna).
-define(YZ_SVC_NAME, yokozuna).
-define(YZ_META_INDEXES, {yokozuna, indexes}).
-define(YZ_META_SCHEMAS, {yokozuna, schemas}).
-define(YZ_META_EXTRACTORS, {yokozuna, extractors}).
-define(YZ_CAPS_CMD_EXTRACTORS, {yokozuna, extractor_map_in_cmd}).
-define(YZ_CAPS_HANDLE_LEGACY_DEFAULT_BUCKET_TYPE_AAE,
        {yokozuna, handle_legacy_default_bucket_type_aae}).

-define(YZ_ERR_NOT_ENOUGH_NODES,
        "Not enough nodes are up to service this request.").
-define(YZ_ERR_INDEX_NOT_FOUND,
        "No index ~p found.").
-define(YZ_ERR_QUERY_FAILURE,
        "Query unsuccessful check the logs.").

%% Given the `StartTime' calculate the amount of time elapsed in
%% microseconds.
-define(YZ_TIME_ELAPSED(StartTime), timer:now_diff(os:timestamp(), StartTime)).

-define(RS_SVC, riak_search).

-define(SOLR_HOST_CONTEXT, "/internal_solr").

%%%===================================================================
%%% Anti Entropy
%%%===================================================================

%% Directory to store the hashtrees.
-define(YZ_AAE_DIR,
        application:get_env(?YZ_APP_NAME, anti_entropy_data_dir)).
%% How often to check for entropy, in milliseconds.  Defaults to 15
%% seconds.
-define(YZ_ENTROPY_TICK,
        app_helper:get_env(?YZ_APP_NAME, anti_entropy_tick,
            app_helper:get_env(riak_kv, anti_entropy_tick, 15000)
        )).
%% The length of time a tree is considered valid, in milliseconds.  If
%% a tree is older than it is considered expired and must be rebuilt
%% from scratch.  Defaults to 1 week.
-define(YZ_ENTROPY_EXPIRE,
        app_helper:get_env(?YZ_APP_NAME, anti_entropy_expire,
            app_helper:get_env(riak_kv, anti_entropy_expire, 604800000)
        )).
%% The amount of time to wait for progress to be made before declaring
%% a timeout, in milliseconds.
-define(YZ_ENTROPY_TIMEOUT,
        app_helper:get_env(?YZ_APP_NAME, anti_entropy_timeout,
            app_helper:get_env(riak_kv, anti_entropy_timeout, 300000)
        )).
%% The maximum number of concurrent anti-entropy operations such as
%% building trees or performing exchanges.  The default is 2.
-define(YZ_ENTROPY_CONCURRENCY,
        app_helper:get_env(?YZ_APP_NAME, anti_entropy_concurrency,
            app_helper:get_env(riak_kv, anti_entropy_concurrency, 2)
        )).
%% How many trees can be built per time period, in milliseconds:
%% `{Trees, Period}'.  Defaults to 1 tree per hour.
-define(YZ_ENTROPY_BUILD_LIMIT,
        app_helper:get_env(?YZ_APP_NAME, anti_entropy_build_limit,
            app_helper:get_env(riak_kv, anti_entropy_build_limit, {1, 3600000})
        )).

-type hashtree() :: hashtree:hashtree().
-type exchange() :: {p(), {p(), n()}}.
-type exchange_mode() :: automatic | manual.
-type tree() :: pid().
-type trees() :: [{p(), tree()}].
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
-type keydiff() :: hashtree:keydiff().

%%%===================================================================
%%% Riak KV
%%%===================================================================

-ifdef(namespaced_types).
-type yz_dict() :: dict:dict().
-else.
-type yz_dict() :: dict().
-endif.

-type obj() :: riak_object:riak_object().
-type obj_metadata() :: yz_dict().

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
-type extractor_map() :: [{mime_type(), extractor_def()}].

-define(NO_OPTIONS, []).

%%%===================================================================
%%% Logging
%%%===================================================================

-define(DEBUG(Fmt, Args), lager:debug(Fmt, Args)).
-define(ERROR(Fmt), lager:error(Fmt)).
-define(ERROR(Fmt, Args), lager:error(Fmt, Args)).
-define(INFO(Fmt, Args), lager:info(Fmt, Args)).
-define(WARN(Fmt, Args), lager:warning(Fmt, Args)).

%%%===================================================================
%%% Indexes
%%%===================================================================

-type indexes() :: [index_name()].
-type index_name() :: binary().

-type reload_opt() :: {schema, boolean()} | {timeout, ms()}.
-type reload_opts() :: [reload_opt()].
-type reload_errs() :: [{node(), {error, term()}}].

-define(YZ_INDEX_TOMBSTONE, <<"_dont_index_">>).
-define(YZ_INDEX, search_index).

%%%===================================================================
%%% Solr Config
%%%===================================================================

-define(YZ_SOLR_CONFIG_NAME, "solr.xml").
-define(YZ_SOLR_CONFIG_TEMPLATE,
        filename:join([?YZ_PRIV, "template_solr.xml"])).

%%%===================================================================
%%% Schemas
%%%===================================================================

-define(YZ_DEFAULT_SCHEMA_FILE,
        filename:join([?YZ_PRIV, "default_schema.xml"])).
-define(YZ_DEFAULT_SCHEMA_NAME, <<"_yz_default">>).
-define(YZ_SCHEMA_BUCKET, <<"_yz_schema">>).

-type raw_schema() :: binary().
%% xmerl does not export these types,
%% so we just define them to be equal
%% to their records
-type xmlElement() :: #xmlElement{}.
-type xmlDocument() :: #xmlDocument{}.
-type schema() :: xmlElement() | xmlDocument().
-type schema_name() :: binary().

%%%===================================================================
%%% Solr Fields
%%%===================================================================

%% ID
-define(YZ_ID_FIELD, '_yz_id').
-define(YZ_ID_FIELD_S, "_yz_id").
-define(YZ_ID_FIELD_B, <<"_yz_id">>).
-define(YZ_ID_FIELD_XML, ?YZ_FIELD_XML(?YZ_ID_FIELD_S, "true")).
-define(YZ_ID_FIELD_XPATH, "/schema/fields/field[@name=\"_yz_id\" and @type=\"_yz_str\" and @indexed=\"true\" and @stored=\"true\" and @required=\"true\" and @multiValued=\"false\"]").

%% Entropy Data
-define(YZ_ED_FIELD, '_yz_ed').
-define(YZ_ED_FIELD_S, "_yz_ed").
-define(YZ_ED_FIELD_XML, ?YZ_FIELD_XML(?YZ_ED_FIELD_S)).
-define(YZ_ED_FIELD_XPATH, "/schema/fields/field[@name=\"_yz_ed\" and @type=\"_yz_str\" and @indexed=\"true\" and @multiValued=\"false\"]").

%% First Partition Number
-define(YZ_FPN_FIELD, '_yz_fpn').
-define(YZ_FPN_FIELD_S, "_yz_fpn").
-define(YZ_FPN_FIELD_B, <<"_yz_fpn">>).
-define(YZ_FPN_FIELD_XML, ?YZ_FIELD_XML(?YZ_FPN_FIELD_S)).
-define(YZ_FPN_FIELD_XPATH, "/schema/fields/field[@name=\"_yz_fpn\" and @type=\"_yz_str\" and @indexed=\"true\" and @multiValued=\"false\"]").

%% Sibling VTags
-define(YZ_VTAG_FIELD, '_yz_vtag').
-define(YZ_VTAG_FIELD_S, "_yz_vtag").
-define(YZ_VTAG_FIELD_B, <<"_yz_vtag">>).
-define(YZ_VTAG_FIELD_XML, ?YZ_FIELD_XML(?YZ_VTAG_FIELD_S)).
-define(YZ_VTAG_FIELD_XPATH, "/schema/fields/field[@name=\"_yz_vtag\" and @type=\"_yz_str\" and @indexed=\"true\" and @multiValued=\"false\"]").

%% Partition Number
-define(YZ_PN_FIELD, '_yz_pn').
-define(YZ_PN_FIELD_S, "_yz_pn").
-define(YZ_PN_FIELD_B, <<"_yz_pn">>).
-define(YZ_PN_FIELD_XML, ?YZ_FIELD_XML(?YZ_PN_FIELD_S)).
-define(YZ_PN_FIELD_XPATH, "/schema/fields/field[@name=\"_yz_pn\" and @type=\"_yz_str\" and @indexed=\"true\" and @multiValued=\"false\"]").

%% Riak key
-define(YZ_RK_FIELD, '_yz_rk').
-define(YZ_RK_FIELD_S, "_yz_rk").
-define(YZ_RK_FIELD_B, <<"_yz_rk">>).
-define(YZ_RK_FIELD_XML, ?YZ_FIELD_XML(?YZ_RK_FIELD_S)).
-define(YZ_RK_FIELD_XPATH, "/schema/fields/field[@name=\"_yz_rk\" and @type=\"_yz_str\" and @indexed=\"true\" and @stored=\"true\" and @multiValued=\"false\"]").

%% Riak bucket type
-define(YZ_RT_FIELD, '_yz_rt').
-define(YZ_RT_FIELD_S, "_yz_rt").
-define(YZ_RT_FIELD_B, <<"_yz_rt">>).
-define(YZ_RT_FIELD_XML, ?YZ_FIELD_XML(?YZ_RT_FIELD_S)).
-define(YZ_RT_FIELD_XPATH, "/schema/fields/field[@name=\"_yz_rt\" and @type=\"_yz_str\" and @indexed=\"true\" and @stored=\"true\" and @multiValued=\"false\"]").

%% Riak bucket
-define(YZ_RB_FIELD, '_yz_rb').
-define(YZ_RB_FIELD_S, "_yz_rb").
-define(YZ_RB_FIELD_B, <<"_yz_rb">>).
-define(YZ_RB_FIELD_XML, ?YZ_FIELD_XML(?YZ_RB_FIELD_S)).
-define(YZ_RB_FIELD_XPATH, "/schema/fields/field[@name=\"_yz_rb\" and @type=\"_yz_str\" and @indexed=\"true\" and @stored=\"true\" and @multiValued=\"false\"]").

%% Riak extraction error
-define(YZ_ERR_FIELD, '_yz_err').
-define(YZ_ERR_FIELD_S, "_yz_err").
-define(YZ_ERR_FIELD_B, <<"_yz_err">>).
-define(YZ_ERR_FIELD_XML, ?YZ_FIELD_XML(?YZ_ERR_FIELD_S)).
-define(YZ_ERR_FIELD_XPATH, "/schema/fields/field[@name=\"_yz_err\" and @type=\"_yz_str\" and @indexed=\"true\" and @multiValued=\"false\"]").

-define(YZ_IS_YZ_FIELD_S(Name),
        Name == ?YZ_ID_FIELD_S orelse
        Name == ?YZ_ED_FIELD_S orelse
        Name == ?YZ_FPN_FIELD_S orelse
        Name == ?YZ_VTAG_FIELD_S orelse
        Name == ?YZ_PN_FIELD_S orelse
        Name == ?YZ_RK_FIELD_S orelse
        Name == ?YZ_RB_FIELD_S orelse
        Name == ?YZ_ERR_FIELD_S).

-define(YZ_UK_XML, {uniqueKey, [?YZ_ID_FIELD_S]}).

-define(YZ_FIELD_XML(Name), ?YZ_FIELD_XML(Name, false)).
-define(YZ_FIELD_XML(Name, Required),
        {field,
         [{name,Name},
          {type,?YZ_STR_FT_S},
          {indexed,"true"},
          {stored,"true"},
          {required,Required}],
         []}).

%% Field Types
-define(YZ_STR_FT_S, "_yz_str").
-define(YZ_STR_FT_XML,
        {fieldType,
         [{name,?YZ_STR_FT_S},
          {class,"solr.StrField"},
          {sortMissingLast,"true"}],
         []}).
-define(YZ_IS_YZ_FT_S(Name), Name == ?YZ_STR_FT_S).
-define(YZ_STR_FT_XPATH, "/schema/types/fieldType[@name=\"_yz_str\" and @class=\"solr.StrField\" and @sortMissingLast=\"true\"]").
