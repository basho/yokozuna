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

-module(yz_entropy).
-compile(export_all).
-include("yokozuna.hrl").

-define(HOUR_SEC, 60 * 60).
-define(DAY_SEC, ?HOUR_SEC * 24).

%% @doc This module contains functionality related to entropy.

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Iterate all the entropy data in `Index' calling `Fun' for
%%      every 100 entries.
-spec iterate_entropy_data(index_name(), list(), function()) -> ok.
iterate_entropy_data(Index, Filter, Fun) ->
    case yz_solr:ping(Index) of
        true ->
            DateTime = calendar:now_to_universal_time(os:timestamp()),
            Before = to_datetime(minus_period(DateTime, [{mins, 5}])),
            Filter2 = [{before, Before},
                       {continuation, none},
                       {limit, 100}|Filter],
            ED = yz_solr:entropy_data(Index, Filter2),
            iterate_entropy_data(Index, Filter2, Fun, ED);
        false ->
            ok
    end.

iterate_entropy_data(Index, Filter, Fun, #entropy_data{more=true,
                                                       continuation=Cont,
                                                       pairs=Pairs}) ->
    lists:foreach(Fun, Pairs),
    Filter2 = lists:keyreplace(continuation, 1, Filter, {continuation, Cont}),
    ED = yz_solr:entropy_data(Index, Filter2),
    iterate_entropy_data(Index, Filter2, Fun, ED);
iterate_entropy_data(_, _, Fun, #entropy_data{more=false,
                                              pairs=Pairs}) ->
    lists:foreach(Fun, Pairs).

%% @doc Minus Period from DateTime.
%%
%% @spec minus_period(DateTime, Period::Period) -> DateTime
%%   DateTime = {{Year, Month, Day}, {Hour, Min, Sec}}
%%   Period = {period, {days, integer()}, {hours, integer()}}
%%
minus_period(DateTime, Periods) ->
    Days = proplists:get_value(days, Periods, 0),
    Hours = proplists:get_value(hours, Periods, 0),
    Minutes = proplists:get_value(minutes, Periods, 0),
    PeriodSecs = (Days * ?DAY_SEC) + (Hours * ?HOUR_SEC) + (Minutes * 60),
    DateTimeSecs = calendar:datetime_to_gregorian_seconds(DateTime),
    calendar:gregorian_seconds_to_datetime(DateTimeSecs - PeriodSecs).

%% @doc Convert `erlang:now/0' or calendar datetime type to an ISO8601
%%      datetime.
%%
%% TODO: rename to_iso8601
-spec to_datetime(timestamp() | datetime()) -> iso8601().
to_datetime({_Mega, _Secs, _Micro}=Now) ->
    to_datetime(calendar:now_to_datetime(Now));
to_datetime({{Year, Month, Day}, {Hour, Min, Sec}}) ->
    list_to_binary(io_lib:format("~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0B",
                                 [Year,Month,Day,Hour,Min,Sec])).
