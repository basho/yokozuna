%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%-------------------------------------------------------------------

%% @doc Test that checks through various HTTP calls and methods
%%      related to Yokozuna
%% @end

-module(yz_search_http).

-compile(export_all).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CFG,
        [
         {riak_core,
          [
           {ring_creation_size, 8}
          ]},
         {yokozuna,
          [
           {enabled, true}
          ]}
        ]).
-define(NO_HEADERS, []).
-define(NO_BODY, <<>>).
-define(INDEX, <<"test_search_http">>).
-define(TYPE, <<"data_foo">>).
-define(BUCKET, {?TYPE, <<"test_search_http">>}).

confirm() ->
    [Node1|_] = Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    ok = yz_rt:create_bucket_type(Node1, ?TYPE),
    ok = yz_rt:create_index_http(Cluster, ?INDEX),
    yz_rt:set_index(Node1, ?BUCKET, ?INDEX),
    HP = hd(yz_rt:host_entries(rt:connection_info(Cluster))),
    Key = <<"Problem">>,
    Value = <<"FOR YOU">>,
    ok = yz_rt:http_put(HP, ?BUCKET, Key, Value),
    yz_rt:commit(Cluster, ?INDEX),
    URL = yz_rt:search_url(HP, ?INDEX),

    test_search_get_and_post_query(HP, URL, ?INDEX),
    test_post_as_get(URL),
    test_post_as_get_with_wrong_content_types(URL),
    test_get_and_post_no_params(URL),

    pass.

test_search_get_and_post_query(HP, URL, Index) ->
    {ok, "200", _, _} = yz_rt:search(HP, Index, "*", "*"),
    ?assert(yz_rt:search_expect(yokozuna, HP, Index, "text", "F*", 1)),
    CT = {content_type, "application/x-www-form-urlencoded"},
    Headers = [CT],
    Params = [{q, "text:F*"}, {wt, "json"}],
    Body = mochiweb_util:urlencode(Params),
    lager:info("Check post with content-type ~s and message body"),
    {ok, "200", _, R} = yz_rt:http(post, URL, Headers, Body),
    ?assert(yz_rt:verify_count(1, R)).

test_post_as_get(URL) ->
    CT = {content_type, "application/x-www-form-urlencoded"},
    Headers = [CT],
    {ok, "200", _, R} = yz_rt:http(post, URL ++ "?q=text:F*&wt=json",
                                   Headers, ?NO_BODY),
    ?assert(yz_rt:verify_count(1, R)).

test_post_as_get_with_wrong_content_types(URL) ->
    CT1 = {content_type, "application/x-www-form-urlen"},
    lager:info("Check misspelled content-type ~s", [element(2, CT1)]),
    Headers1 = [CT1],
    {ok, Status1, _, _} = yz_rt:http(post, URL ++ "?q=text:F*&wt=json",
                                    Headers1, ?NO_BODY),
    ?assertEqual("415", Status1),

    CT2 = {content_type, "application/json"},
    Headers2 = [CT2],
    lager:info("Check non-applicable content-type ~s", [element(2, CT2)]),
    Params = [{q, "text:F*"}, {wt, "json"}],
    Body = mochiweb_util:urlencode(Params),
    {ok, Status2, _, _} = yz_rt:http(post, URL, Headers2, Body),
    ?assertEqual("415", Status2).

test_get_and_post_no_params(URL) ->
    {ok, Status1, _, _} = yz_rt:http(get, URL, ?NO_HEADERS, ?NO_BODY),

    CT = {content_type, "application/x-www-form-urlencoded"},
    Headers = [CT],
    {ok, Status2, _, _} = yz_rt:http(post, URL, Headers, ?NO_BODY),
    ?assertEqual("200", Status1),
    ?assertEqual("200", Status2).
