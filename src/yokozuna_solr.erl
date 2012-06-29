-module(yokozuna_solr).
-compile(export_all).
-include("yokozuna.hrl").

-define(CORE_ALIASES, [{index_dir, instanceDir},
                       {cfg_file, config},
                       {java_lib_dir, "property.yz_java_lib_dir"},
                       {schema_file, schema}]).
-define(DEFAULT_URL, "http://localhost:8983/solr").
-define(DEFAULT_VCLOCK_N, 1000).

%% @doc This module provides the interface for making calls to Solr.
%%      All interaction with Solr should go through this API.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Perform Core related actions.
-spec core(atom(), proplists:proplist()) -> ok.
core(Action, Props) ->
    BaseURL = base_url() ++ "/admin/cores",
    Action2 = convert_action(Action),
    Params = proplists:substitute_aliases(?CORE_ALIASES,
                                          [{action, Action2}|Props]),
    Encoded = mochiweb_util:urlencode(Params),
    Opts = [{response_format, binary}],
    URL = BaseURL ++ "?" ++ Encoded,

    case ibrowse:send_req(URL, [], get, [], Opts) of
        {ok, "200", _Headers, _Body} ->
            ok;
        X ->
            throw({error_calling_solr, create_core, X})
    end.

%% @doc Get `N' key-vclock pairs that occur before the `Before'
%%      timestamp.
-spec get_vclocks(iso8601()) -> solr_vclocks().
get_vclocks(Before) ->
    get_vclocks(Before, none, ?DEFAULT_VCLOCK_N).

get_vclocks(Before, Continue, N) when N > 0 ->
    BaseURL = base_url() ++ "/merkle_tree",
    Params = [{before, Before}, {wt, json}, {n, N}],
    Params2 = if Continue == none -> Params;
                 true -> [{continue, Continue}|Params]
              end,
    Encoded = mochiweb_util:urlencode(Params2),
    Opts = [{response_format, binary}],
    URL = BaseURL ++ "?" ++ Encoded,
    case ibrowse:send_req(URL, [], get, [], Opts) of
        {ok, "200", _Headers, Body} ->
            R = mochijson2:decode(Body),
            More = json_get_key(<<"more">>, R),
            Continuation = get_continuation(More, R),
            Pairs = get_pairs(R),
            make_solr_vclocks(More, Continuation, Pairs);
        X ->
            {error, X}
    end.

%% @doc Index the given `Docs'.
index(Docs) ->
    esolr:add(Docs).

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Get the base URL.
base_url() ->
    app_helper:get_env(base_url, ?YZ_APP_NAME, ?DEFAULT_URL).

convert_action(create) -> "CREATE".

%% @doc Get the continuation value if there is one.
get_continuation(false, _R) ->
    none;
get_continuation(true, R) ->
    json_get_key(<<"continuation">>, R).

get_pairs(R) ->
    Docs = json_get_key(<<"docs">>, get_response(R)),
    [to_pair(DocStruct) || DocStruct <- Docs].

to_pair({struct, [{_,DocId},{_,Base64VClock}]}) ->
    {DocId, Base64VClock}.

get_response(R) ->
    json_get_key(<<"response">>, R).

%% @doc Given a "struct" created by `mochijson2:decode' get the given
%%      `Key' or throw if not found.
json_get_key(Key, {struct, PL}) ->
    case proplists:get_value(Key, PL) of
        undefined -> {error, not_found, Key, PL};
        Val -> Val
    end;
json_get_key(_Key, Term) ->
    throw({error, "json_get_key: Term not a struct", Term}).

make_solr_vclocks(More, Continuation, Pairs) ->
    #solr_vclocks{more=More, continuation=Continuation, pairs=Pairs}.
