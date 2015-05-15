%% -*- encoding: utf-8 -*-
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
%% -------------------------------------------------------------------

%%
%%  @doc    Test helper operations.
%%
-module(yz_test).

-export([read_file/2]).

-include_lib("kernel/include/file.hrl").

-spec read_file(FilePath :: file:name_all(), Encoding ::  unicode:encoding())
        -> {ok, Binary :: binary()} | {error, Reason :: term()}.
%%
%%  @doc    Mimics {@link file:read_file/1}, but accomodates character encoding.
%%
%%  Behaves per the specification of {@link file:read_file/1}, with the
%%  additional capability to translate character data in the file being read
%%  into bytes in the same way that a list of characters would be converted to
%%  bytes by `unicode:characters_to_binary(..., Encoding)'.
%%
read_file(FilePath, latin1) ->
    file:read_file(FilePath);
read_file(FilePath, Encoding) ->
    case file:read_file_info(FilePath, [{time, posix}]) of
        {ok, FI} ->
            read_file(FilePath, FI#file_info.size, Encoding);
        Err ->
            Err
    end.

read_file(_FilePath, 0, _Encoding) ->
    {ok, <<>>};
read_file(FilePath, Size, Encoding) ->
    case file:open(FilePath, [binary, read, {encoding, Encoding}]) of
        {ok, FD} ->
            RD = io:get_chars(FD, '', Size),
            ok = file:close(FD),
            case RD of
                {error, _} = Error ->
                    Error;
                eof ->
                    % This shouldn't be possible, since we handle zero-length
                    % files explicitly. If it *can* happen, I missed something
                    % in the fine OTP documentation.
                    {error, {bug, {?MODULE, read_file, 3}}};
                Data ->
                    {ok, Data}
            end;
        Err ->
            Err
    end.
