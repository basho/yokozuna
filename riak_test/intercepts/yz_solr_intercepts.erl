-module(yz_solr_intercepts).
-compile(export_all).

-type index_name() :: binary().

-define(M, yz_solr_orig).
-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).

-spec slow_cores() -> {ok, []}.
slow_cores() ->
    timer:sleep(6000),
    {ok, []}.

-spec entropy_data_cant_complete(index_name(), list()) -> {error, term()}.
entropy_data_cant_complete(Core, Filter) ->
    Params = [{wt, json}|Filter] -- [{continuation, none}],
    Params2 = proplists:substitute_aliases([{continuation, continue},
                                            {limit,n}], Params),
    Opts = [{response_format, binary}],
    URL = ?FMT("~s/~s/entropy_data?~s",
               [yz_solr:base_url(), Core, mochiweb_util:urlencode(Params2)]),
    case ibrowse:send_req(URL, [], get, [], Opts, 0) of
        Error ->
            {error, Error}
    end.

index_batch_call_orig(Core, Ops) ->
    ?M:index_batch_orig(Core, Ops).

index_batch_returns_other_error(_Core, _Ops) ->
    {errlr, other, "Failed to index docs"}.
