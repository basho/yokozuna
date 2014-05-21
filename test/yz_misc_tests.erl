%%--------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
%%--------------------------------------------------------------------

%% @doc EUnit and Exercise yz_misc.
-module(yz_misc_tests).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

should_copy_skip_test() ->
    Thisfile = atom_to_list(?MODULE) ++ ".erl",
    ?assertNot(yz_misc:should_copy(skip, Thisfile, Thisfile)),
    ?assert(yz_misc:should_copy(skip, "<nofile>", "<nofile>")),
    ?assert(yz_misc:should_copy(skip, Thisfile, "<nofile>")).

should_copy_update_test() ->
    Thisfile = atom_to_list(?MODULE) ++ ".erl",
    %% same file and no file acts the same as skip
    ?assertNot(yz_misc:should_copy(update, Thisfile, Thisfile)),
    ?assert(yz_misc:should_copy(update, "<nofile>", "<nofile>")),
    ?assert(yz_misc:should_copy(update, Thisfile, "<nofile>")),
    %% No file to update
    ?assertNot(yz_misc:should_copy(update, "<nofile>", Thisfile)),
    %% Compare timestamps
    Newfile = lists:flatten(io_lib:format("~p-~p",[
                erlang:phash2(make_ref()),node()])),
    try
        ok = file:write_file(Newfile, "data"),
        ?assert(yz_misc:should_copy(update, Newfile, Thisfile)),
        ?assertNot(yz_misc:should_copy(update, Thisfile, Newfile))
    after
        file:delete(Newfile)
    end.

should_copy_overwrite_test() ->
    Thisfile = atom_to_list(?MODULE) ++ ".erl",
    ?assert(yz_misc:should_copy(overwrite, Thisfile, Thisfile)),
    ?assert(yz_misc:should_copy(overwrite, "<nofile>", "<nofile>")),
    ?assert(yz_misc:should_copy(overwrite, Thisfile, "<nofile>")).

should_verify_name_test() ->
    ?assertEqual({ok, <<"just-fine">>}, yz_index:verify_name(<<"just-fine">>)),
    ?assertEqual({error, invalid_name}, yz_index:verify_name(<<"bad/slash">>)),
    ?assertEqual({error, invalid_name}, yz_index:verify_name(<<"out-of-range-", 129>>)),
    ?assertEqual({error, invalid_name}, yz_index:verify_name(<<"out-of-range-", 31>>)),
    ?assertEqual({ok, <<"just-in-range- ">>}, yz_index:verify_name(<<"just-in-range-", 32>>)).
