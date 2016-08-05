%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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
-module(yz_solr_upgrade).

-export([main/0]).
%% tmp
-compile(export_all).

-include_lib("xmerl/include/xmerl.hrl").

-define(LUCENE_MATCH_4_7_VERSION, "4.7").
-define(LUCENE_MATCH_4_10_4_VERSION, "4.10.4").

-define(DRY_RUN, 'riak-search-dryrun').
-define(RIAK_ROOT_DIR, 'riak-root').
-define(INDICES, 'riak-search-indices').
-define(HELP, 'riak-help').


main() ->
    Args = init:get_arguments(),
    %log_debug("Args: ~p", [Args]),
    case get_help(Args) of
        true ->
            print_help();
        false ->
            upgrade(Args)
    end.


print_help() ->
    io:format(
        "Syntax: yz_solr_upgrade:main([Options]), where options is among:~n"
        "\t-~s - Print this help~n"
        "\t-~s - Riak root directory~n"
        "\t-~s - Dry run -- verify and print actions only~n"
        "", [
            atom_to_list(?HELP),
            atom_to_list(?RIAK_ROOT_DIR),
            atom_to_list(?DRY_RUN)
        ]
    ).

upgrade(Args) ->
    RiakRootDir = get_riak_root_dir(Args),
    %log_debug("FDUSHIN> RiakRootDir: ~p", [RiakRootDir]),
    DryRun = get_dry_run(Args),
    try
        %%
        %% get the effective config via the riak command.  This will tell
        %% us where the PlatformDataDir is.  This must be an absolute path
        %% that points to a directory that exists.
        %%
        EffectiveConfigStr = os:cmd("riak config effective"),
        EffectiveConfig = parse_config(EffectiveConfigStr),
        %log_debug("FDUSHIN> EffectiveConfig: ~p", [EffectiveConfig]),
        PlatformDataDir = RiakRootDir ++ "/" ++ proplists:get_value("platform_data_dir", EffectiveConfig),
        case {is_relative(PlatformDataDir), filelib:is_dir(PlatformDataDir)} of
            {true, _} ->
                Msg = io_lib:format(
                    "The platform data directory is a relative path: ~s  "
                    "Specify the (absolute) root path to the RIAK release.",
                    [PlatformDataDir]
                ),
                throw(Msg);
            {_, false} ->
                Msg = io_lib:format(
                    "The specified platform data directory does not exist: ~s  "
                    "Specify the (absolute) root path to the RIAK release.",
                    [PlatformDataDir]
                ),
                throw(Msg);
            _ -> ok
        end,
        %%
        %% Warn the user if we are doing an upgrade and the search.dist_query.enable
        %% flag is still set.
        %%
        DistQueryEnabled = proplists:get_value("search.dist_query.enable", EffectiveConfig),
        case DistQueryEnabled of
            "on" ->
                log_warning(
                    "The search.dist_query.enable flag is set to \"on\".  "
                    "Enabling this flag during upgrade may result in inconsistent "
                    "search results during upgrade.  Users are advised to disable "
                    "this flag during upgrade until all repairs have completed."
                );
            _ ->
                ok
        end,
        TimestampStr = timestamp_str(),
        %%
        %% move/backup the yz_temp directory (which may contain cached Solr files)
        %%
        YZTempDir = substitute(
            "platform_data_dir", PlatformDataDir,
            proplists:get_value("search.temp_dir", EffectiveConfig)
        ),
        mv_yz_temp(YZTempDir, TimestampStr, DryRun),
        %%
        %% move/backup the YZ anti-entropy directory
        %%
        mv_yz_aae_dir(PlatformDataDir, TimestampStr, DryRun),
        %%
        %% Upgrade the luceneMatchVersions in the solrconfig.xml files
        %% for the indices to repair
        %%
        SearchRootDir = substitute(
            "platform_data_dir", PlatformDataDir,
            proplists:get_value("search.root_dir", EffectiveConfig)
        ),
        IndicesToRepair = case get_indices(Args) of
            all ->
                find_indices(SearchRootDir);
            Indices ->
                Indices
        end,
        [update_index(SearchRootDir, Index, TimestampStr, DryRun) ||
            Index <- IndicesToRepair],
        ok
    catch
        _:E ->
            log_error(io_lib:format("~s", [E]))
    end.


parse_config(PropertiesStr) ->
    Lines = string:tokens(PropertiesStr, "\n"),
    lists:foldl(
        fun ([$# | _Rest], Accum) ->
                Accum;
            (Line, Accum) ->
                KeyEndPos = string:str(Line, " = "),
                Key = string:substr(Line, 1, KeyEndPos - 1),
                Value = string:substr(Line, KeyEndPos + 3),
                [{Key, Value} | Accum]
        end,
        [],
        Lines
    ).

substitute(Substitutand, Substitution, String) ->
    Find = "$(" ++ Substitutand ++ ")",
    Pos = string:str(String, Find),
    case Pos of
        0 -> String;
        _ ->
            string:substr(String, 1, Pos - 1)
                ++ Substitution
                ++ substitute(Substitutand, Substitution,
                    string:substr(String, Pos + length(Find)))
    end.

mv_yz_temp(FromPath, TimestampStr, DryRun) ->
    ToPath = io_lib:format("~s-~s", [FromPath, TimestampStr]),
    backup_dir(FromPath, ToPath, DryRun, fun do_mv_yz_temp/3).

do_mv_yz_temp(FromPath, ToPath, false) ->
    case file:rename(FromPath, ToPath) of
        ok ->
            log_info("Moved yz_temp directory from ~s to ~s", [FromPath, ToPath]),
            ok;
        {error, Reason} ->
            log_error(
                "Failed to move yz_temp directory from ~s to ~s.  Reason: ~p",
                [FromPath, ToPath, Reason])
    end;
do_mv_yz_temp(FromPath, ToPath, true) ->
    log_info("DryRun: Would move yz_temp directory from ~s to ~s", [FromPath, ToPath]),
    ok.

mv_yz_aae_dir(DataDir, TimestampStr, DryRun) ->
    FromPath = io_lib:format("~s/yz_anti_entropy", [DataDir]),
    ToPath = io_lib:format("~s-~s", [FromPath, TimestampStr]),
    backup_dir(FromPath, ToPath, DryRun, fun do_mv_yz_aae_dir/3).

do_mv_yz_aae_dir(FromPath, ToPath, false) ->
    case file:rename(FromPath, ToPath) of
        ok ->
            log_info("Moved yz_anti_entropy directory from ~s to ~s", [FromPath, ToPath]),
            ok;
        {error, Reason} ->
            log_warning(
                "Failed to move yz_anti_entropy directory from ~s to ~s.  Reason: ~p",
                [FromPath, ToPath, Reason])
    end;
do_mv_yz_aae_dir(FromPath, ToPath, true) ->
    log_info("DryRun: Would move yz_anti_entropy directory from ~s to ~s", [FromPath, ToPath]),
    ok.

backup_dir(FromPath, ToPath, DryRun, F) ->
    case {filelib:is_dir(FromPath), not exists(ToPath)} of
        {true, true} ->
            F(FromPath, ToPath, DryRun);
        {false, _} ->
            log_error("~s does not exist or is not a directory!", [FromPath]);
        {_, false} ->
            log_error("~s already exists!", [ToPath])
    end.

exists(Path) ->
    filelib:is_dir(Path) orelse filelib:is_file(Path).


update_index(YZRootDir, Index, TimestampStr, DryRun) ->
    update_solr_config(YZRootDir, Index, TimestampStr, DryRun),
    mv_yz_index_data_dir(YZRootDir, Index, TimestampStr, DryRun).


update_solr_config(YZRootDir, Index, TimestampStr, DryRun) ->
    FromPath = io_lib:format("~s/~s/conf/solrconfig.xml", [YZRootDir, Index]),
    ToPath = io_lib:format("~s-~s", [FromPath, TimestampStr]),
    do_update_solr_config(FromPath, ToPath, DryRun).

do_update_solr_config(FromPath, ToPath, false) ->
    case file:rename(FromPath, ToPath) of
        ok ->
            try
                {Doc, _Rest} = xmerl_scan:file(ToPath),
                Doc2 = replace_version(Doc, ?LUCENE_MATCH_4_7_VERSION, ?LUCENE_MATCH_4_10_4_VERSION),
                ExportIoList = xmerl:export_simple([Doc2], xmerl_xml),
                {ok, IOF} = file:open(FromPath,[write]),
                io:format(IOF, "~s", [ExportIoList]),
                file:close(IOF),
                log_info("Backed up ~s to ~s and updated luceneMatchVersion to ~s", [FromPath, ToPath, ?LUCENE_MATCH_4_10_4_VERSION])
            catch
                _:E  ->
                    log_error("An Error occurred updating luceneMatchVersion in file ~s  Reason: ~p", [FromPath, E])
            end,
            ok;
        {error, Reason} ->
            log_error("Failed to move ~s to ~s  Reason: ~p",
                [FromPath, ToPath, Reason])
    end,
    ok;
do_update_solr_config(FromPath, ToPath, true) ->
    log_info("DryRun: Would backed up ~s to ~s and updated luceneMatchVersion to ~s", [FromPath, ToPath, ?LUCENE_MATCH_4_10_4_VERSION]),
    ok.


mv_yz_index_data_dir(SearchRootDir, Index, TimestampStr, DryRun) ->
    FromPath = io_lib:format("~s/~s/data", [SearchRootDir, Index]),
    ToPath = io_lib:format("~s-~s", [FromPath, TimestampStr]),
    backup_dir(FromPath, ToPath, DryRun, fun do_mv_yz_index_data_dir/3).

do_mv_yz_index_data_dir(FromPath, ToPath, false) ->
    case file:rename(FromPath, ToPath) of
        ok ->
            log_info("Moved index data directory from ~s to ~s", [FromPath, ToPath]),
            ok;
        {error, Reason} ->
            log_warning(
                "Failed to move index data directory from ~s to ~s.  Reason: ~p",
                [FromPath, ToPath, Reason])
    end;
do_mv_yz_index_data_dir(FromPath, ToPath, true) ->
    log_info("DryRun: Would move index data directory from ~s to ~s", [FromPath, ToPath]),
    ok.





timestamp_str() ->
    Now = erlang:now(),
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:now_to_local_time(Now),
    io_lib:format("~b-~2..0b-~2..0bT~2..0b.~2..0b.~2..0b",
        [Year, Month, Day, Hour, Minute, Second]).

%%
%% @doc Replace the contents
%%
replace_version(#xmlElement{content = Contents} = Doc, FromVersion, ToVersion) ->
    Doc#xmlElement{
        content = [replace_content(Content, FromVersion, ToVersion) || Content <- Contents]
    }.

%%
%% @doc If a content is an XML Element with the tag `luceneMatchVersion` and
%% the content is a singleton with XML Text (which it should be), then
%% replace the value of the text with the specified version.
%% Otherwise, just leave it be.
%%
replace_content(
    #xmlElement{name = luceneMatchVersion,
        content = [#xmlText{} = Content]} = Element,
    _FromVersion, ToVersion) ->
    Element#xmlElement{
        content = [Content#xmlText{value = ToVersion}]
    };
replace_content(Element, _FromVersion, _ToVersion) ->
    Element.


is_relative([$/|_]) -> false;
is_relative(_) -> true.

get_indices(Args) ->
    get_single_arg_val(?INDICES, Args, all).

get_riak_root_dir(Args) ->
    get_single_arg_val(?RIAK_ROOT_DIR, Args, "").

get_dry_run(Args) ->
    get_single_arg_val(?DRY_RUN, Args, false).

get_help(Args) ->
    get_single_arg_val(?HELP, Args, false).

get_single_arg_val(Key, Args, Default) ->
    case proplists:get_value(Key, Args) of
        [] -> true;
        [H|_] -> H;
        undefined -> Default
    end.


find_indices(SearchRootDir) ->
    {ok, Files} = file:list_dir(SearchRootDir),
    [File || File <- Files, filelib:is_dir(SearchRootDir ++ "/" ++ File)].

%% TODO Configure lager to print to the console
log_debug(Fmt, Args) ->
    log_debug(io_lib:format(Fmt, Args)).
log_debug(Msg) ->
    io:format("DEBUG: ~s~n", [Msg]).

log_info(Fmt, Args) ->
    log_info(io_lib:format(Fmt, Args)).
log_info(Msg) ->
    io:format("INFO: ~s~n", [Msg]).

log_error(Fmt, Args) ->
    log_error(io_lib:format(Fmt, Args)).
log_error(Msg) ->
    io:format("ERROR: ~s~n", [Msg]).

log_warning(Fmt, Args) ->
    log_warning(io_lib:format(Fmt, Args)).
log_warning(Msg) ->
    io:format("WARNING: ~s~n", [Msg]).
