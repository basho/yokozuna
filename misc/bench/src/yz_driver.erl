%% @doc A Basho Bench driver for Yokozuna.
-module(yz_driver).

%% Callbacks
-export([new/1,
         run/4]).
-compile(export_all).

-include_lib("basho_bench/include/basho_bench.hrl").
-type cardinality() :: pos_integer().
-record(state, {add_2i, default_field, fruits, pb_conns, index, bucket, iurls, surls}).
-define(DONT_VERIFY, dont_verify).

-define(M100,   100000000).
-define(M10,    10000000).
-define(M1,     1000000).
-define(K100,   100000).
-define(K10,    10000).
-define(K1,     1000).
-define(FRUITS,
        [{?M100, "safou rimu medlar mayapple muskmelon kitembilla hackberry gac"},
         {?M10, "pumpkin rollinia soncoya toyon yew wampee ugni tamarind"},
         {?M1, "blueberry apricot guava feijoa jackfruit jambul limequat pulasan"},
         {?K100, "apple grape orange pineapple strawberry kiwi huito lychee"},
         {?K10, "avocado raspberry persimmon blackberry cherry tomato huckleberry muscadine"},
         {?K1, "clementine lime lemon melon plum pear gooseberry honeydew"},
         {100, "marang nutmeg olive pecan peanut tangerine barbadine duku"},
         {10, "nunga nance mulberry langsat karonda kumquat bacupari bael"},
         {1, "korlan jocote genip elderberry citron jujube abiu babaco"}]).

-define(INT_TO_BIN(I), list_to_binary(integer_to_list(I))).

%% ====================================================================
%% API
%% ====================================================================

-type bucket() :: bucket() | {bucket(), bucket()}.

-spec bucket_path(bucket()) -> binary().
bucket_path({Type, Name}) ->
    <<"/types/",Type/binary,"/buckets/",Name/binary,"/keys">>;
bucket_path(Name) ->
    <<"/buckets/",Name/binary,"/keys">>.

-spec search_path(bucket()) -> binary().
search_path({BucketType, _}) ->
    <<"/solr/",BucketType/binary,"/select">>;
search_path(BucketName) ->
    <<"/solr/",BucketName/binary,"/select">>.

-spec bucket_type(bucket()) -> binary().
bucket_type({Type, _}) ->
    Type;
bucket_type(Name) ->
    Name.

start_apps(Apps) ->
    [application:start(App) || App <- Apps].

new(_Id) ->
    start_apps([crypto, asn1, public_key, ssl, ibrowse]),
    Secure = basho_bench_config:get(secure, false),
    User = basho_bench_config:get(user, "user"),
    Password = basho_bench_config:get(password, "password"),
    Cert = basho_bench_config:get(cert, "rootcert.pem"),
    case Secure of
        true ->
            ?INFO("Security enabled: ~s ~s ~s", [User, Password, Cert]);
        false ->
            ok
    end,
    Bucket = basho_bench_config:get(bucket, {<<"test">>, <<"test">>}),
    Index = basho_bench_config:get(index, bucket_type(Bucket)),
    HTTP = basho_bench_config:get(http_conns, [{"127.0.0.1", 8098}]),
    PB = basho_bench_config:get(pb_conns, [{"127.0.0.1", 8087}]),
    DefaultField = basho_bench_config:get(default_field, <<"text">>),
    BPath = basho_bench_config:get(bucket_path, bucket_path(Bucket)),
    SPath = basho_bench_config:get(search_path, search_path(Index)),
    Add2i = basho_bench_config:get(add_2i, false),
    IURLs = array:from_list(lists:map(make_url(BPath), HTTP)),
    SURLs = array:from_list(lists:map(make_url(SPath), HTTP)),
    Conns = array:from_list(lists:map(make_conn(Secure, User, Password, Cert), PB)),
    N = length(HTTP),
    M = length(PB),

    {ok, #state{add_2i=Add2i,
                pb_conns={Conns, {0,M}},
                bucket=Bucket,
                default_field=DefaultField,
                index=Index,
                iurls={IURLs, {0,N}},
                surls={SURLs, {0,N}}}}.

run(search, _KeyGen, ValGen, S=#state{default_field=DF, surls=URLs}) ->
    Base = get_base(URLs),
    {Field, [Term]} = ValGen(search),
    Qry = ?FMT("~s:~s", [Field, Term]),
    Params = mochiweb_util:urlencode([{<<"q">>, Qry}, {df, DF}]),
    URL = ?FMT("~s?~s", [Base, Params]),
    S2 = S#state{surls=wrap(URLs)},
    case http_get(URL) of
        {ok, _} -> {ok, S2};
        {error, Reason} -> {error, Reason, S2}
    end;

run({search, Qry, Params, Opts}, KG, VG, S) ->
    run({search, Qry, Params, ?DONT_VERIFY, Opts}, KG, VG, S);

run({search, Qry, Params, Expected, Opts}, _, _, S=#state{default_field=DF, surls=URLs}) ->
    case proplists:get_value(paginate, Opts) of
        undefined ->
            Base = get_base(URLs),
            Params2 = mochiweb_util:urlencode([{q, Qry},
                                               {wt, <<"json">>},
                                               {df, DF}|Params]),
            URL = ?FMT("~s?~s", [Base, Params2]),
            S2 = S#state{surls=wrap(URLs)},
            case {Expected, http_get(URL)} of
                {?DONT_VERIFY, {ok,_}} ->
                    {ok, S2};
                {_, {ok, Body}} ->
                    check_numfound(Qry, Body, Expected, S2);
                {_, {error, Reason}} ->
                    {error, Reason, S2}
            end;
        cursor ->
            paginate_via_cursor(Qry, Params, Expected, S);
        start_and_rows ->
            paginate_via_start_and_rows(Qry, Params, Expected, S)
    end;

run({random_fruit_search, Params, MaxTerms, Cs={MinCardinality, MaxCardinality}, Opts},
    K, V, S=#state{fruits=undefined}) ->
    %% NOTE: This clause only runs once and then caches the state. All
    %% subsequent calls use the cached state. Performing expensive
    %% operations in this function is okay.
    S2 = S#state{fruits=gen_fruits(MinCardinality, MaxCardinality)},
    run({random_fruit_search, Params, MaxTerms, Cs, Opts}, K, V, S2);

run({random_fruit_search, Params, MaxTerms, _, Opts},
    K, V, S=#state{fruits={Len, Fruits, Cards}}) ->
    %% Select a random number of terms, NumTerms, from the shuffled
    %% Fruits list.
    NumTerms = random:uniform(MaxTerms),
    Offset = random:uniform(Len),
    TermList = lists:sublist(Fruits, Offset, NumTerms),
    CardList = lists:sublist(Cards, Offset, NumTerms),
    ExpectedNumFound = lists:min(CardList),
    Query = string:join(TermList, " AND "),
    run({search, Query, Params, ExpectedNumFound, Opts}, K, V, S);

run({index, CT}, _KeyGen, ValGen, S=#state{iurls=URLs}) ->
    Base = get_base(URLs),
    {Key, Line} = ValGen(index),
    Key2 = mochiweb_util:quote_plus(Key),
    URL = ?FMT("~s/~s", [Base, Key2]),
    S2 = S#state{iurls=wrap(URLs)},
    case http_put(URL, CT, Line) of
        ok -> {ok, S2};
        {error, Reason} -> {error, Reason, S2}
    end;

run(load_fruit, KeyValGen, _, S=#state{iurls=URLs}) ->
    Base = get_base(URLs),
    {Key, Val} = KeyValGen(),
    URL = ?FMT("~s/~p", [Base, Key]),
    S2 = S#state{iurls=wrap(URLs)},
    case http_put(URL, "text/plain", Val) of
        ok -> {ok, S2};
        {error, Reason} -> {error, Reason, S2}
    end;

run(load_fruit_pb, KeyValGen, _, S=#state{bucket=Bucket, pb_conns=Conns, add_2i=Add2i}) ->
    Conn = get_conn(Conns),
    {Key, Val} = KeyValGen(),
    Obj = riakc_obj:new(Bucket, ?INT_TO_BIN(Key), list_to_binary(Val), "text/plain"),
    Obj2 = case Add2i of
               true ->
                   MD1 = riakc_obj:get_update_metadata(Obj),
                   Idx = {{integer_index, "field1_int"}, [Key]},
                   MD2 = riakc_obj:set_secondary_index(MD1, Idx),
                   riakc_obj:update_metadata(Obj, MD2);
               false ->
                   Obj
           end,
    S2 = S#state{pb_conns=wrap(Conns)},
    case riakc_pb_socket:put(Conn, Obj2) of
        ok -> {ok, S2};
        Err -> {error, Err, S2}
    end;

run({random_fruit_search_pb, Params, MaxTerms, Cs={MinCardinality, MaxCardinality}},
    K, V, S=#state{fruits=undefined}) ->
    S2 = S#state{fruits=gen_fruits(MinCardinality, MaxCardinality)},
    run({random_fruit_search_pb, Params, MaxTerms, Cs}, K, V, S2);

run({random_fruit_search_pb, Params, MaxTerms, _}, K, V, S=#state{fruits={Len, Fruits, Cards}}) ->
    NumTerms = random:uniform(MaxTerms),
    Offset = random:uniform(Len),
    TermList = lists:sublist(Fruits, Offset, NumTerms),
    CardList = lists:sublist(Cards, Offset, NumTerms),
    ExpectedNumFound = lists:min(CardList),
    Query = list_to_binary(string:join(TermList, " AND ")),
    run({search_pb, Query, Params, ExpectedNumFound}, K, V, S);

run({search_pb, Query, Params, Expected}, _, _,
    S=#state{default_field=DF, index=Index, pb_conns=Conns}) ->
    Conn = get_conn(Conns),
    S2 = S#state{pb_conns=wrap(Conns)},
    case {Expected, search_pb(Conn, Index, Query, [{df,DF}|Params])} of
        {?DONT_VERIFY, {ok, _, _, _}} ->
            {ok, S2};
        {_, {ok, _, _, NumFound}} ->
            case NumFound =:= Expected of
                true ->
                    {ok, S2};
                false ->
                    ?ERROR("Query ~p expected ~p got ~p", [Query, Expected, NumFound]),
                    {error, {num_found, Expected, NumFound}, S2}
            end;
        {_, {error, Reason}} ->
            {error, Reason, S2}
    end;

run(search_pb, _, QueryGen,
    S=#state{default_field=DF, index=Index, pb_conns=Conns}) ->
    Conn = get_conn(Conns),
    Query = QueryGen(),
    S2 = S#state{pb_conns=wrap(Conns)},
    case search_pb(Conn, Index, Query, [{df,DF}]) of
        {ok, _, _, _} -> {ok, S2};
        {error, Reason} -> {error, Reason, S2}
    end;

run(show, KeyGen, _ValGen, S) ->
    {K, V} = KeyGen(),
    ?INFO("~p: ~p~n", [K, V]),
    {ok, S}.

-spec search_pb(pid(), binary(), binary()) ->
                       {ok, [{binary(),binary()}], number(), integer()} | term().
search_pb(Conn, Index, Query) ->
    search_pb(Conn, Index, Query, []).

search_pb(Conn, Index, Query, Opts) ->
    case riakc_pb_socket:search(Conn, Index, Query, Opts) of
        {ok, {search_results,Fields,MaxScore,NumFound}} ->
            {ok, Fields,MaxScore,NumFound};
        Other ->
            Other
    end.

%% ====================================================================
%% Key Gens
%% ====================================================================

%% @doc Allow to have different valgen depending on operation.
valgen(Id, Path, Fields, Schema) ->
    N = basho_bench_config:get(concurrent),
    if Id == N ->
            {ok, _} = yz_file_terms:start_ts(Path, Fields, Schema),
            {ok, _} = yz_file_terms:start_ls(Path);
       true -> ok
    end,
    fun ?MODULE:valgen_i/1.

valgen_i(index) ->
    yz_file_terms:get_line();
valgen_i(search) ->
    yz_file_terms:get_ft().

mfa_valgen(Id, LoadMFA, ReadMFA) ->
    if Id == 1 ->
            {ok, _} = yz_file_terms:start_mfa(LoadMFA, ReadMFA);
       true ->
            ok
    end,
    fun ?MODULE:mfa_valgen_i/1.

mfa_valgen_i(index) ->
    case yz_file_terms:read_mfa(index) of
        finished ->
            throw({stop, empty_valgen});
        Val ->
            Val
    end.

key_range(Id, NumKeys, NumWorkers) ->
    Range = NumKeys div NumWorkers,
    Start = Range * (Id - 1) + 1,
    End = Range * Id,
    if NumWorkers == Id ->
            {Start, NumKeys - Start};
       true ->
            {Start, End - Start}
    end.

%% generates key and value because value is based on key
fruit_key_val_gen(Id) ->
    fruit_key_val_gen(Id, ?K100).

fruit_key_val_gen(Id, NumKeys) ->
    Fruits2 = [{N, combine(?FRUITS, N)}
               || N <- [1, 10, 100, ?K1, ?K10, ?K100, ?M1, ?M10, ?M100]],
    Workers = basho_bench_config:get(concurrent),
    {Start, NumToWrite} = key_range(Id, NumKeys, Workers),
    Ref = make_ref(),

    ?DEBUG("ID ~p generating range ~p to ~p\n", [Id, Start, Start + NumToWrite]),
    fun() ->
            %% Need to add 1 to NumToWrite because sequential gen
            %% doesn't write last value
            K = basho_bench_keygen:sequential_int_generator(Ref, NumToWrite + 1,
                                                            Id, false) + Start,
            V = first_large_enough(K, Fruits2),
            {K, V}
    end.

always(_Id, Val) ->
    fun() -> Val end.

%% ====================================================================
%% Private
%% ====================================================================

-spec check_numfound(binary(), binary(), integer(), #state{}) ->
                            {ok, #state{}} | {error, any(), #state{}}.
check_numfound(Qry, Body, Expected, S) ->
    Struct = mochijson2:decode(Body),
    NumFound = get_path(Struct, [<<"response">>, <<"numFound">>]),
    case Expected == NumFound of
        true ->
            {ok, S};
        false ->
            ?ERROR("Query ~p expected ~p got ~p~nBody: ~s~n",
                   [Qry, Expected, NumFound, Body]),
            {error, {num_found, Expected, NumFound}, S}
    end.

combine(Fruits, N) ->
    string:join([Str || {Count, Str} <- Fruits, Count >= N], " ").

%% @private
%%
%% @doc Filter a set of pairs by it's cardinality.
%%
%% L = [{1, "apple pear"}, {10, "orange kiwi"}, {100, "banana peach"}].
%% filter_by_cardinality(L, 10).
%%
%% => [{1, "apple pear"}, {10, "orange kiwi"}]
-spec filter_by_cardinality([{cardinality(), list()}], cardinality(), cardinality()) ->
                                   [{cardinality(), list()}].
filter_by_cardinality(Pairs, MinCardinality, MaxCardinality) ->
    [P || {C,_}=P <- Pairs, C >= MinCardinality andalso C =< MaxCardinality].

first_large_enough(K, [{Count, Str}|Fruits]) ->
    if Count >= K -> Str;
       true -> first_large_enough(K, Fruits)
    end.

get_base({URLs, {I,_}}) -> array:get(I, URLs).

get_conn({Conns, {I,_}}) -> array:get(I, Conns).

-spec gen_fruits(cardinality(), cardinality()) ->
                        {pos_integer(), [string()], [cardinality()]}.
gen_fruits(MinCardinality, MaxCardinality) ->
    %% Make a list of shuffled fruits to use for qurying
    Fruits = tokenize(filter_by_cardinality(?FRUITS, MinCardinality, MaxCardinality)),
    ShuffledFruits0 = shuffle(Fruits),
    Len = length(ShuffledFruits0),
    %% Double it to fake wrap-around (for Offset + NumTerms > length(Fruits))
    ShuffledFruits = ShuffledFruits0 ++ ShuffledFruits0,
    {ShuffledCards, ShuffledTokens} = lists:unzip(ShuffledFruits),
    {Len, ShuffledTokens, ShuffledCards}.

get_path({struct, PL}, Path) ->
    get_path(PL, Path);
get_path(PL, [Name]) ->
    case proplists:get_value(Name, PL) of
        {struct, Obj} -> Obj;
        Val -> Val
    end;
get_path(PL, [Name|Path]) ->
    get_path(proplists:get_value(Name, PL), Path).

http_get(URL) ->
    case ibrowse:send_req(URL, [], get, [], [{response_format,binary}]) of
        {ok, "200", _, Body} -> {ok, Body};
        {ok, Status, _, _} -> {error, {bad_status, Status, URL}};
        {error, Reason} -> {error, Reason}
    end.

http_put(URL, CT, Body) ->
    case ibrowse:send_req(URL, [{content_type, CT}], put, Body, [{content_type, CT}]) of
        {ok, "200", _, _} -> ok;
        {ok, "201", _, _} -> ok;
        {ok, "204", _, _} -> ok;
        {ok, Status, _, Resp} -> {error, {bad_status, Status, URL, Resp}};
        {error, Reason} -> {error, Reason}
    end.

make_url(Path) ->
    fun({IP, Port}) -> ?FMT("http://~s:~w~s", [IP, Port, Path]) end.

make_conn(Secure, User, Password, Cert) ->
    fun({IP, Port}) ->
            case Secure of
                true ->
                    Opts = [{credentials, User, Password},
                            {cacertfile, Cert}],
                    case riakc_pb_socket:start_link(IP, Port, Opts) of
                        {ok, Conn} ->
                            Conn;
                        Err ->
                            ?ERROR("Failed to make connection: ~p", [Err]),
                            Err
                    end;
                false ->
                    {ok, Conn} = riakc_pb_socket:start_link(IP, Port),
                    Conn
            end
    end.

%% @private
-spec paginate_via_cursor(string(), [{atom(), binary()}],
                          non_neg_integer(), #state{}) ->
                                 {ok, #state{}} |
                                 {error, term(), #state{}}.
paginate_via_cursor(Query, Params, ExpectedNumFound, S) ->
    DF=S#state.default_field,
    Params2 = [{q, Query},
               {df, DF},
               {wt, <<"json">>}|Params],
    case paginate_via_cursor_(Query, Params2, S, {<<"*">>, 0}) of
        {ok, Seen} ->
            case true of
                true ->
                    {ok, S};
                false ->
                    ?ERROR("Query '~s' expected ~B but got ~B",
                           [Query, ExpectedNumFound, Seen]),
                    {error, {num_found, ExpectedNumFound, Seen}, S}
            end;
        Err ->
            Err
    end.

paginate_via_cursor_(Query, Params, S, {Cursor, NumSeen}) ->
    URLs=S#state.surls,
    Base = get_base(URLs),
    Params2 = lists:keystore(cursorMark, 1, Params, {cursorMark,Cursor}),
    URL = ?FMT("~s?~s", [Base, mochiweb_util:urlencode(Params2)]),
    case http_get(URL) of
        {ok, Body} ->
            Struct = mochijson2:decode(Body),
            Docs = length(get_path(Struct, [<<"response">>, <<"docs">>])),
            NewCursor =  get_path(Struct, [<<"nextCursorMark">>]),
            NewNumSeen = NumSeen + Docs,
            case NewCursor /= Cursor of
                true ->
                    paginate_via_cursor_(Query, Params2, S, {NewCursor, NewNumSeen});
                false ->
                    {ok, NewNumSeen}
            end;
        {error, Reason} ->
            {error, Reason, S}
    end.

paginate_via_start_and_rows(Query, Params, ExpectedNumFound, S) ->
    DF = S#state.default_field,
    Params2 = [{q, Query},
               {df, DF},
               {wt, <<"json">>}|Params],
    case paginate_via_start_and_rows_(Query, Params2, S, {0, 0}) of
        {ok, Seen} ->
            case ExpectedNumFound == Seen of
                true ->
                    {ok, S};
                false ->
                    ?ERROR("Query '~s' expected ~B but got ~B",
                           [Query, ExpectedNumFound, Seen]),
                    {error, {num_found, ExpectedNumFound, Seen}, S}
            end;
        Err ->
            Err
    end.

paginate_via_start_and_rows_(Query, Params, S, {Start, NumSeen}) ->
    URLs=S#state.surls,
    Base = get_base(URLs),
    Params2 = lists:keystore(start, 1, Params, {start, Start}),
    Rows = proplists:get_value(rows, Params, 10),
    URL = ?FMT("~s?~s", [Base, mochiweb_util:urlencode(Params2)]),
    case http_get(URL) of
        {ok, Body} ->
            Struct = mochijson2:decode(Body),
            Docs = length(get_path(Struct, [<<"response">>, <<"docs">>])),
            NewNumSeen = NumSeen + Docs,
            case Docs > 0 of
                true ->
                    paginate_via_start_and_rows_(Query, Params2, S, {Start + Rows, NewNumSeen});
                false ->
                    {ok, NewNumSeen}
            end;
        {error, Reason} ->
            {error, Reason, S}
    end.

%% @private
%%
%% @doc Suffle the list.
-spec shuffle(list()) -> list().
shuffle(L) ->
    L0 = lists:keysort(1, [{random:uniform(1000), X} || X <- L]),
    [X || {_,X} <- L0].

%% @private
%%
%% @doc Tokenize the string value for each pair. Each resulting token
%% is paired with its cardinality.
%%
%% L = [{1, "apple pear"}, {10, "orange kiwi"}].
%%
%% [{1, "apple"}, {1, "pear"}, {10, "orange"}, {10, "kiwi"}].
-spec tokenize([{integer(), Tokens :: string()}]) ->
                      [{integer(), Token :: string()}].
tokenize(Pairs) ->
    L = [begin
             Tokens = string:tokens(Str, " "),
             Cards = lists:duplicate(length(Tokens), C),
             lists:zip(Cards, Tokens)
         end || {C, Str} <- Pairs],
    lists:append(L).

wrap({URLs, {I,N}}) when I == N - 1 -> {URLs, {0,N}};
wrap({URLs, {I,N}}) -> {URLs, {I+1,N}}.
