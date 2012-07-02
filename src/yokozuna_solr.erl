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

commit() ->
    BaseURL = base_url() ++ "/update",
    XML = encode_commit(),
    Params = [{commit, true}],
    Encoded = mochiweb_util:urlencode(Params),
    URL = BaseURL ++ "?" ++ Encoded,
    Headers = [{content_type, "text/xml"}],
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, Headers, post, XML, Opts) of
        {ok, "200", _, _} -> ok;
        Err -> throw({"Failed to commit", Err})
    end.

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

delete(DocID) ->
    BaseURL = base_url() ++ "/update",
    XML = encode_delete(DocID),
    Params = [],
    Encoded = mochiweb_util:urlencode(Params),
    URL = BaseURL ++ "?" ++ Encoded,
    Headers = [{content_type, "text/xml"}],
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, Headers, post, XML, Opts) of
        {ok, "200", _, _} -> ok;
        Err -> throw({"Failed to delete doc", DocID, Err})
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
    BaseURL = base_url() ++ "/update",
    Doc = {add, [], lists:map(fun encode_doc/1, Docs)},
    XML = xmerl:export_simple([Doc], xmerl_xml),
    Params = [],
    Encoded = mochiweb_util:urlencode(Params),
    URL = BaseURL ++ "?" ++ Encoded,
    Headers = [{content_type, "text/xml"}],
    Opts = [{response_format, binary}],
    case ibrowse:send_req(URL, Headers, post, XML, Opts) of
        {ok, "200", _, _} -> ok;
        Err -> throw({"Failed to index docs", Docs, Err})
    end.

%% @doc Return boolean based on ping response from Solr.
-spec ping() -> boolean().
ping() ->
    URL = base_url() ++ "/admin/ping",
    case ibrowse:send_req(URL, [], get) of
        {ok, "200", _, _} -> true;
        _ -> false
    end.

start(Dir) ->
    SolrPort = app_helper:get_env(?YZ_APP_NAME, solr_port, ?YZ_DEFAULT_SOLR_PORT),
    _Pid = spawn_link(?MODULE, solr_process, [Dir, SolrPort]),
    ok.

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Get the base URL.
base_url() ->
    Port = app_helper:get_env(?YZ_APP_NAME, port, ?YZ_DEFAULT_SOLR_PORT),
    "http://localhost:" ++ Port ++ "/solr".

convert_action(create) -> "CREATE".

%% TODO: Encoding functions copied from esolr, redo this.
encode_commit() ->
	xmerl:export_simple([{commit, []}], xmerl_xml).

encode_delete({id,Id})->
	xmerl:export_simple([{delete, [], [{id, [], [Id]}]}], xmerl_xml).

encode_doc({doc, Fields}) ->
	{doc, [], lists:map(fun encode_field/1,Fields)};

encode_doc({doc, Boost, Fields}) ->
	{doc, [{boost, Boost}], lists:map(fun encode_field/1, Fields)}.

encode_field({Name, Value}) when is_binary(Value)->
	{field, [{name,Name}], [[Value]]};

encode_field({Name,Value}) ->
	{field, [{name,Name}], [Value]};

encode_field({Name,Value,Boost}) when is_binary(Value)->
	{field, [{name,Name}, {boost,Boost}], [[Value]]};

encode_field({Name,Value,Boost}) ->
	{field, [{name,Name}, {boost,Boost}], [Value]}.

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

solr_process(Dir, SolrPort) ->
    Cmd = "java -Dsolr.solr.home=. -Djetty.port=" ++ SolrPort ++ " -jar start.jar",
    io:format("running: ~p~n", [Cmd]),
    Port = open_port({spawn, Cmd}, [{cd, Dir}, exit_status]),
    solr_process_loop(Dir, Port).

solr_process_loop(_Dir, Port) ->
    receive
        {Port, {data, Data}} -> solr_process_loop(_Dir, Port);
        {Port, {exit_status, 0}} -> throw({solr_exited, 0});
        {Port, {exit_status, Num}} -> throw({solr_exited, Num})
    end.
