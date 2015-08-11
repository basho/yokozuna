%% @doc Test the index adminstration API in various ways.
-module(yz_pb).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_yokozuna_pb.hrl").
-include("yokozuna.hrl").

-define(CFG, [{yokozuna, [{enabled, true}]}]).
-define(CT_JSON, {"Content-Type", "application/json"}).

-define(ALTERNATE_SCHEMA_CONTENT, <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_err\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"age_i\" type=\"int\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"groups_s\" type=\"string\" indexed=\"true\" stored=\"true\" multiValued=\"true\"/>
</fields>
<uniqueKey>_yz_id</uniqueKey>
<types>
    <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"string\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"int\" class=\"solr.TrieIntField\" precisionStep=\"0\" positionIncrementGap=\"0\" />
</types>
</schema>">>).

confirm() ->
    random:seed(now()),
    Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    confirm_admin_schema(Cluster),
    confirm_admin_bad_schema_name(Cluster),
    confirm_admin_index(Cluster),
    confirm_admin_bad_index_name(Cluster),
    confirm_basic_search(Cluster),
    confirm_fl_search_without_score(Cluster),
    confirm_fl_search_without_score_without_sort(Cluster),
    confirm_encoded_search(Cluster),
    confirm_search_to_test_max_score_defaults(Cluster),
    confirm_multivalued_field(Cluster),
    confirm_multivalued_field_json_array(Cluster),
    confirm_multivalued_field_with_high_n_val(Cluster),
    confirm_stored_fields(Cluster),
    confirm_search_non_existent_index(Cluster),
    confirm_search_with_spaced_key(Cluster),
    pass.

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

host_entries(ClusterConnInfo) ->
    [proplists:get_value(http, I) || {_,I} <- ClusterConnInfo].

bucket_url({Host,Port}, {BType, BName}, Key) ->
    ?FMT("http://~s:~B/types/~s/buckets/~s/keys/~s",
         [Host, Port, BType, BName, Key]).

create_index(Cluster, BucketType, Index) ->
    create_index(Cluster, BucketType, Index, 3).

create_index(Cluster, BucketType, Index, UseDefaultSchema) when
      is_boolean(UseDefaultSchema) ->
    create_index(Cluster, BucketType, Index, 3, UseDefaultSchema);
create_index(Cluster, BucketType, Index, Nval) when is_integer(Nval) ->
    create_index(Cluster, BucketType, Index, Nval, true).

create_index(Cluster, BucketType, Index, Nval, UseDefaultSchema) ->
    Node = select_random(Cluster),
    [{Host, Port}] = host_entries(rt:connection_info([Node])),
    lager:info("create_index ~s for bucket type ~s [~p]", [Index, BucketType, {Host, Port}]),
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    if
        UseDefaultSchema ->
            lager:info("Using Default Schema"),
            SchemaName = ?YZ_DEFAULT_SCHEMA_NAME;
        true ->
            lager:info("Using Custom Schema ?ALTERNATE_SCHEMA_CONTENT"),
            SchemaName = <<"alternate_schema">>,
            ok = yz_rt:store_schema(Pid, SchemaName, ?ALTERNATE_SCHEMA_CONTENT),
            ok = yz_rt:wait_for_schema(Cluster, SchemaName,
                                       ?ALTERNATE_SCHEMA_CONTENT)
    end,
    NvalT = {n_val, Nval},
    %% set index in props with the same name as the bucket
    _ = riakc_pb_socket:create_search_index(Pid, Index, SchemaName, [NvalT]),
    ?assertEqual(ok, yz_rt:wait_for_index(Cluster, Index)),
    %% Check that the index exists
    {ok, IndexData} = riakc_pb_socket:get_search_index(Pid, Index),
    ?assertEqual([{index, Index}, {schema, SchemaName}, NvalT], IndexData),

    %% Add the index to the bucket props
    yz_rt:set_bucket_type_index(Cluster, BucketType, Index, Nval),
    yz_rt:wait_for_bucket_type(Cluster, BucketType),
    riakc_pb_socket:stop(Pid),
    ok.

store_and_search(Cluster, Bucket, Key, Body, Search, Params) ->
    store_and_search(Cluster, Bucket, Key, Body, "text/plain", Search, Params).

store_and_search(Cluster, Bucket, Key, Body, CT, Search, Params) ->
    {BType, Bucket2} = Bucket,
    {Host, Port} = select_random(host_entries(rt:connection_info(Cluster))),
    URL = bucket_url({Host, Port}, {BType, mochiweb_util:quote_plus(Bucket2)},
                     mochiweb_util:quote_plus(Key)),
    lager:info("Storing to bucket ~s", [URL]),
    %% populate a value
    {ok, "204", _, _} = ibrowse:send_req(URL, [{"Content-Type", CT}], put, Body),
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    F = fun(_) ->
        lager:info("Search for ~s [~p:~p]", [Search, Host, Port]),
        {ok,{search_results,R,Score,Found}} =
            riakc_pb_socket:search(Pid, BType, Search, Params),
            ?assertNotEqual(Score, []),
            ?assertNotEqual(Score, 0),
        case Found of
            1 ->
                [{BType,Results}] = R,
                KeyCheck = (Key == binary_to_list(proplists:get_value(<<"_yz_rk">>, Results))),
                ScoreCheck = (Score =/= 0.0),
                KeyCheck and ScoreCheck;
            0 ->
                false
        end
    end,
    yz_rt:wait_until(Cluster, F),
    riakc_pb_socket:stop(Pid),
    ok.

confirm_admin_schema(Cluster) ->
    Name = <<"my_schema">>,
    Node = select_random(Cluster),
    {Host, Port} = yz_rt:riak_pb(hd(rt:connection_info([Node]))),
    lager:info("confirm_admin_schema ~s [~p]", [Name, {Host, Port}]),
    {ok, Pid} = riakc_pb_socket:start_link(Host, Port),
    yz_rt:store_schema(Pid, Name, ?ALTERNATE_SCHEMA_CONTENT),
    yz_rt:wait_for_schema(Cluster, Name, ?ALTERNATE_SCHEMA_CONTENT),
    MissingMessage = riakc_pb_socket:get_search_schema(Pid,<<"not_here">>),
    ?assertEqual({error,<<"notfound">>}, MissingMessage),
    riakc_pb_socket:stop(Pid),
    ok.

confirm_admin_bad_schema_name(Cluster) ->
    Name = <<"bad/name">>,
    Node = select_random(Cluster),
    {Host, Port} = yz_rt:riak_pb(hd(rt:connection_info([Node]))),
    lager:info("confirm_admin_bad_schema_name ~s [~p]", [Name, {Host, Port}]),
    {ok, Pid} = riakc_pb_socket:start_link(Host, Port),
    {error,_} = riakc_pb_socket:create_search_schema(Pid, Name,
                                                     ?ALTERNATE_SCHEMA_CONTENT),
    riakc_pb_socket:stop(Pid),
    ok.

confirm_admin_index(Cluster) ->
    Index = <<"index">>,
    create_index(Cluster, Index, Index, 4),
    Node = select_random(Cluster),
    [{Host, Port}] = host_entries(rt:connection_info([Node])),
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    F = fun(_) ->
                %% Remove index from bucket props and delete it
                yz_rt:remove_index(Node, Index),
                DelResp = riakc_pb_socket:delete_search_index(Pid, Index),
                case DelResp of
                    ok -> true;
                    {error,<<"notfound">>} -> true
                end
    end,
    yz_rt:wait_until(Cluster, F),
    riakc_pb_socket:stop(Pid),
    ok.

confirm_admin_bad_index_name(Cluster) ->
    Name = <<"bad/name">>,
    Node = select_random(Cluster),
    [{Host, Port}] = host_entries(rt:connection_info([Node])),
    lager:info("confirm_admin_bad_index_name ~s [~p]", [Name, {Host, Port}]),
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    {error,_} = riakc_pb_socket:create_search_index(Pid, Name),
    riakc_pb_socket:stop(Pid),
    ok.

confirm_basic_search(Cluster) ->
    Index = <<"basic">>,
    Bucket = {Index, <<"b1">>},
    create_index(Cluster, Index, Index),
    lager:info("confirm_basic_search ~p", [Bucket]),
    Body = "herp derp",
    Params = [{sort, <<"score desc">>}, {fl, ["*","score"]}],
    store_and_search(Cluster, Bucket, "test", Body, <<"text:herp">>, Params).

confirm_fl_search_without_score(Cluster) ->
    Index = <<"fl_search_without_score">>,
    Bucket = {Index, <<"b1">>},
    create_index(Cluster, Index, Index),
    lager:info("confirm_fl_search_without_score ~p", [Bucket]),
    Body = <<"{\"age_i\":5}">>,
    Params = [{sort, <<"age_i asc">>}, {fl, ["*"]}],
    store_and_search(Cluster, Bucket, "test_fl_search_without_score", Body,
                     "application/json", <<"age_i:5">>, Params).

confirm_fl_search_without_score_without_sort(Cluster) ->
    Index = <<"fl_search_without_score_without_sort">>,
    Bucket = {Index, <<"b1">>},
    create_index(Cluster, Index, Index),
    lager:info("confirm_fl_search_without_score ~p", [Bucket]),
    Body = <<"{\"age_i\":5}">>,
    Params = [{fl, ["age_i", "_yz_rk"]}],
    store_and_search(Cluster, Bucket, "test_fl_search_without_score_without_sort",
                     Body, "application/json", <<"age_i:5">>, Params).

confirm_encoded_search(Cluster) ->
    Index = <<"encoded">>,
    Bucket = {Index, <<"b1">>},
    create_index(Cluster, Index, Index),
    lager:info("confirm_encoded_search ~p", [Bucket]),
    Body = "א בְּרֵאשִׁית, בָּרָא אֱלֹהִים, אֵת הַשָּׁמַיִם, וְאֵת הָאָרֶץ",
    Params = [{sort, <<"score desc">>}, {fl, ["_yz_rk"]}],
    store_and_search(Cluster, Bucket, "וְאֵת", Body, <<"text:בָּרָא">>, Params).

confirm_search_to_test_max_score_defaults(Cluster) ->
    Index = <<"basic_for_max_score">>,
    Bucket = {Index, <<"b1">>},
    create_index(Cluster, Index, Index, false),
    lager:info("confirm_search_to_test_max_score_defaults ~p", [Bucket]),
    Body = <<"{\"age_i\":5}">>,
    Params = [{sort, <<"age_i asc">>}],
    store_and_search(Cluster, Bucket, "test_max_score_defaults",
                     Body, "application/json", <<"age_i:5">>, Params).

confirm_multivalued_field(Cluster) ->
    Index = <<"basic">>,
    Bucket = {Index, <<"b1">>},
    lager:info("confirm multiValued=true fields decode properly"),
    Body = <<"{\"name_ss\":\"turner\", \"name_ss\":\"hooch\"}">>,
    Params = [],
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    URL = bucket_url(HP, Bucket, "multivalued"),
    lager:info("Storing to bucket ~s", [URL]),
    {Host, Port} = HP,
    %% populate a value
    {ok, "204", _, _} = ibrowse:send_req(URL, [?CT_JSON], put, Body),
    yz_rt:commit(Cluster, Index),
    Search = <<"name_ss:turner">>,
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    F = fun(_) ->
        case riakc_pb_socket:search(Pid, Index, Search, Params) of
            {ok,{search_results,[{Index,Fields}], _Score, Found}} ->
                ?assert(lists:member({<<"name_ss">>,<<"turner">>}, Fields)),
                ?assert(lists:member({<<"name_ss">>,<<"hooch">>}, Fields)),
                case Found of
                    1 -> true;
                    0 -> false
                end;
            {ok, {search_results, [], _Score, 0}} ->
                lager:info("Search for multivalued_field has not yet yielded data"),
                false
            end
        end,
    yz_rt:wait_until(Cluster, F),
    riakc_pb_socket:stop(Pid).

confirm_multivalued_field_json_array(Cluster) ->
    Index = <<"multifunarray">>,
    Bucket = {Index, <<"b2">>},
    lager:info("confirm multiValued=true array fields decode properly"),
    create_index(Cluster, Index, Index, false),
    Body = <<"{\"groups_s\":[\"3304cf79\", \"abe155cf\"]}">>,
    Params = [],
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    URL = bucket_url(HP, Bucket, "multivalued_array"),
    lager:info("Storing to bucket ~s", [URL]),
    {Host, Port} = HP,
    {ok, "204", _, _} = ibrowse:send_req(URL, [?CT_JSON], put, Body),
    yz_rt:commit(Cluster, Index),
    Search = <<"groups_s:3304cf79">>,
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    F = fun(_) ->
        case riakc_pb_socket:search(Pid, Index, Search, Params) of
            {ok,{search_results,[{Index,Fields}], _Score, Found}} ->
                ?assert(lists:member({<<"groups_s">>,<<"3304cf79">>},
                                     Fields)),
                case Found of
                    1 -> true;
                    0 -> false
                end;
            {ok, {search_results, [], _Score, 0}} ->
                lager:info("Search for multivalued_field_json_array has not yet yielded data"),
                false
            end
        end,
    yz_rt:wait_until(Cluster, F),
    riakc_pb_socket:stop(Pid).

confirm_multivalued_field_with_high_n_val(Cluster) ->
    Index = <<"high_n_val">>,
    Bucket = {Index, <<"b3">>},
    lager:info("confirm high_n_val should not affect document indexes"),
    create_index(Cluster, Index, Index, 5, false),
    Body = <<"{\"groups_s\":[\"3304cf79\", \"abe155cf\"]}">>,
    Params = [],
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    URL = bucket_url(HP, Bucket, "high_n_val_test"),
    lager:info("Storing to bucket ~s", [URL]),
    {Host, Port} = HP,
    {ok, "204", _, _} = ibrowse:send_req(URL, [?CT_JSON], put, Body),
    yz_rt:commit(Cluster, Index),
    Search = <<"groups_s:3304cf79">>,
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    F = fun(_) ->
        case riakc_pb_socket:search(Pid, Index, Search, Params) of
            {ok,{search_results,[{Index,Fields}], _Score, Found}} ->
                ?assert(lists:member({<<"groups_s">>,<<"3304cf79">>}, Fields)),
                ?assert(lists:member({<<"groups_s">>,<<"abe155cf">>}, Fields)),

                case Found of
                    1 -> true;
                    0 -> false
                end;
            {ok, {search_results, [], _Score, 0}} ->
                lager:info("Search for multivalued_field_with_high_n_val has not yet yielded data"),
                false
            end
        end,
    yz_rt:wait_until(Cluster, F),
    riakc_pb_socket:stop(Pid).

confirm_search_non_existent_index(Cluster) ->
    BadIndex = <<"does_not_exist">>,
    Search = <<"name_ss:nobody">>,
    {Host, Port} = select_random(host_entries(rt:connection_info(Cluster))),
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    lager:info("Confirm searching for non-existent index:~p", [BadIndex]),
    {error, Err} = riakc_pb_socket:search(Pid, BadIndex, Search, []),
    ?assertEqual(<<"No index <<\"does_not_exist\">> found.">>, Err),
    riakc_pb_socket:stop(Pid).

confirm_stored_fields(Cluster) ->
    Index = <<"stored_fields">>,
    Bucket = {Index, <<"b1">>},
    lager:info("Confirm stored fields"),
    create_index(Cluster, Index, Index),
    Body = <<"{\"bool_b\":true, \"float_tf\":3.14}">>,
    Params = [],
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    URL = bucket_url(HP, Bucket, "stored"),
    lager:info("Storing to bucket ~s", [URL]),
    {Host, Port} = HP,
    {ok, "204", _, _} = ibrowse:send_req(URL, [?CT_JSON], put, Body),
    yz_rt:commit(Cluster, Index),
    Search = <<"float_tf:3.14">>,
    {ok, Pid} = riakc_pb_socket:start_link(Host, (Port-1)),
    F = fun(_) ->
                case riakc_pb_socket:search(Pid, Index, Search, Params) of
                    {ok, {search_results,[{Index,Fields}], _Score, Found}} ->
                        ?assertEqual(<<"true">>, proplists:get_value(<<"bool_b">>, Fields)),
                        ?assertEqual(3.14,
                            ?BIN_TO_FLOAT(proplists:get_value(<<"float_tf">>, Fields))),
                        ?assertEqual(Index, proplists:get_value(<<"_yz_rt">>, Fields)),
                        ?assertEqual(<<"b1">>, proplists:get_value(<<"_yz_rb">>, Fields)),
                        case Found of
                            1 -> true;
                            0 -> false
                        end;
                    {ok, {search_results, [], _Score, 0}} ->
                        lager:info("Search for stored_fields has not yet yielded data"),
                        false
                end
        end,
    yz_rt:wait_until(Cluster, F),
    riakc_pb_socket:stop(Pid).

confirm_search_with_spaced_key(Cluster) ->
    Index = <<"basic_for_spaced_key">>,
    Bucket = {Index, <<"b 1">>},
    Key = "test spaced key",
    create_index(Cluster, Index, Index),
    lager:info("confirm_search_with_spaced_key ~p", [Bucket]),
    Body = <<"{\"foo_i\":5}">>,
    Params = [{sort, <<"age_i asc">>}],
    store_and_search(Cluster, Bucket, Key,
                     Body, "application/json", <<"foo_i:5">>, Params).
