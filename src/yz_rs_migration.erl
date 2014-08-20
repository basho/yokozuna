%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
%% @doc This code is only needed for riak search migration and can be
%% pulled once riak search is removed from riak.

-module(yz_rs_migration).
-compile(export_all).
-include("yokozuna.hrl").

%% @doc Determine if Riak Search is enabled.
-spec is_riak_search_enabled() -> boolean().
is_riak_search_enabled() ->
    app_helper:get_env(?RS_SVC, enabled, false).

%% @doc Remove Riak Search pre-commit hook from all buckets when Riak
%% Search is disabled.
%%
%% Previous versions of Riak had a bug in `set_bucket' which caused
%% bucket fixups to leak into the raw ring.  If a user is upgrading
%% from one of these versions, and enabled search on a bucket, then
%% the Riak Searc hook will be in the raw ring.  After migrating to
%% Yokozuna these hooks must be removed to avoid errors.
-spec strip_rs_hooks(IsRSEnabled :: boolean(), ring()) -> ok.
strip_rs_hooks(true, _) ->
    ok;
strip_rs_hooks(false, Ring) ->
    Buckets = riak_core_ring:get_buckets(Ring),
    strip_rs_hooks_2(Ring, Buckets).

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
