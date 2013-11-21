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

-module(yz_app).
-behaviour(application).
-export([start/2, stop/1]). % prevent compile warnings
-compile(export_all).
-include("yokozuna.hrl").

%% 27 is message type rpbsearchqueryreq
%% 28 is message type rpbsearchqueryresp
-define(QUERY_SERVICES, [{yz_pb_search, 27, 28}]).
-define(ADMIN_SERVICES, [{yz_pb_admin, 54, 60}]).

%%%===================================================================
%%% Callbacks
%%%===================================================================

start(_StartType, _StartArgs) ->
    Enabled = ?YZ_ENABLED,
    case yz_sup:start_link(Enabled) of
        {ok, Pid} ->
            maybe_setup(Enabled),
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    ok = riak_api_pb_service:deregister(?QUERY_SERVICES),
    ok = riak_api_pb_service:deregister(?ADMIN_SERVICES),
    ok.

maybe_setup(false) ->
    ok;
maybe_setup(true) ->
    Ring = yz_misc:get_ring(raw),
    RSEnabled = yz_misc:is_riak_search_enabled(),
    strip_rs_hooks(RSEnabled, Ring),
    Routes = yz_wm_search:routes() ++ yz_wm_extract:routes() ++
	yz_wm_index:routes() ++ yz_wm_schema:routes(),
    yz_misc:add_routes(Routes),
    maybe_register_pb(RSEnabled),
    setup_stats(),
    ok = riak_core:register(yokozuna, [{permissions, [search,admin]}]),
    ok = yz_schema:setup_schema_bucket(),
    ok.

%% @doc Conditionally register PB service IFF Riak Search is not
%%      enabled.
-spec maybe_register_pb(boolean()) -> ok.
maybe_register_pb(true) ->
    lager:info("Not registering Yokozuna protocol buffer services"
               " because Riak Search is enabled as well"),
    ok;
maybe_register_pb(false) ->
    ok = riak_api_pb_service:register(?QUERY_SERVICES),
    ok = riak_api_pb_service:register(?ADMIN_SERVICES).

%% @private
%%
%% @doc Determine if direct stats are being used or not.  If not setup
%% a new sidejob stat resource.  Finally, register Yokozuna stats.
-spec setup_stats() -> ok.
setup_stats() ->
    case app_helper:get_env(riak_kv, direct_stats, false) of
        true -> ok;
        false -> sidejob:new_resource(yz_stat_sj, yz_stat_worker, 10000)
    end,
    ok = riak_core:register(yokozuna, [{stat_mod, yz_stat}]).

%%%===================================================================
%%% EVERYTHING BELOW IS FOR BUG CAUSED BY LEAKED BUCKET FIXUPS.
%%%
%%% Much of this code was copied from `riak_search_kv_hook'.
%%%===================================================================

%% @private
%%
%% @doc Given current pre-commit hook generate a new one with all
%% instances of the Riak Search hook removed.
%%
%% `Changed' - A boolean indicating if the `Precommit' value changed.
%%
%% `NewPrecommit' - The new pre-commit hook.
-spec gen_new_precommit([term()]) -> {Changed :: boolean(),
                                      NewPrecommit :: [term()]}.
gen_new_precommit(Precommit) ->
    %% Strip ALL Riak Search hooks.
    NewPrecommit = lists:filter(fun ?MODULE:not_rs_hook/1, Precommit),
    Changed = not (Precommit =:= NewPrecommit),
    {Changed, NewPrecommit}.

%% @private
%%
%% @doc Retrieve the pre-commit hook from bucket properties.  If it
%% doesn not exist then default to the empty list.
-spec get_precommit([term()]) -> [term()].
get_precommit(BProps) ->
    case proplists:get_value(precommit, BProps, []) of
        X when is_list(X) -> X;
        {struct, _}=X -> [X]
    end.

%% @private
%%
%% @doc Predicate function which returns `true' if the `Hook' is NOT a
%% Riak Search hook.  For use with `lists:filter/2'.
-spec not_rs_hook(term()) -> boolean().
not_rs_hook(Hook) ->
    not (Hook == rs_precommit_def()).

%% @private
%%
%% @doc The definition of the Riak Search pre-commit hook.
-spec rs_precommit_def() -> term().
rs_precommit_def() ->
    {struct, [{<<"mod">>,<<"riak_search_kv_hook">>},
              {<<"fun">>,<<"precommit">>}]}.

%% @private
%%
%% @doc Remove Riak Search pre-commit hook from all buckets when Riak
%% Search is disabled.
%%
%% Previous versions of Riak had a bug in `set_bucket' which caused
%% bucket fixups to leak into the raw ring.  If a user is upgrading
%% from one of these versions, and enabled search on a bucket, then
%% the Riak Searc hook will be in the raw ring.  After migrating to
%% Yokozuna these hooks must be removed to avoid errors.
-spec strip_rs_hooks(boolean(), ring()) -> ok.
strip_rs_hooks(true, _) ->
    ok;
strip_rs_hooks(false, Ring) ->
    Buckets = riak_core_ring:get_buckets(Ring),
    strip_rs_hooks_2(Ring, Buckets).

strip_rs_hooks_2(_Ring, []) ->
    ok;
strip_rs_hooks_2(Ring, [Bucket|Rest]) ->
    BProps = riak_core_bucket:get_bucket(Bucket, Ring),
    Precommit = get_precommit(BProps),
    {Changed, NewPreHook} = gen_new_precommit(Precommit),
    case Changed of
        true ->
            riak_core_bucket:set_bucket(Bucket, [{precommit, NewPreHook}]),
            ok;
        false ->
            ok
    end,
    strip_rs_hooks_2(Ring, Rest).
