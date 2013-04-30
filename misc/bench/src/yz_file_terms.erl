%% @doc This server is used to generate {Field, Terms} pairs.  It is
%%      given a file, a list of fileds, and a schema which it then
%%      uses to iterate the file, line by line, extracting {Field,
%%      Terms} pairs as they are requested.
-module(yz_file_terms).
-behavior(gen_server).
-compile(export_all).

%% Callbacks
-export([init/1, handle_call/3, terminate/2]).

-include_lib("basho_bench/include/basho_bench.hrl").
-define(MB, 1048576).
-record(state, {cache, i, fields, file, finished=false, read_mfa, schema}).

-type field() :: binary().
-type sterm() :: binary().
-type sterms() :: [sterm()].

%% ====================================================================
%% API
%% ====================================================================

%% @doc Return a {Field, Terms} pair.
-spec get_ft() -> {field(), sterms()}.
get_ft() -> get_ft(1).

%% @doc Return a {Field, Terms} pair.  An attempt will be made to
%% return N terms but could be less.
-spec get_ft(pos_integer()) -> {field(), sterms()}.
get_ft(N) ->
    gen_server:call(term_server, {ft, N}).

get_line() ->
    gen_server:call(line_server, line).

read_mfa(Op) ->
     gen_server:call(mfa_server, {read_mfa, Op}).

start_ts(Path, Fields, Schema) ->
    gen_server:start_link({local, term_server}, ?MODULE,
                          [Path, Fields, Schema], []).

start_ls(Path) ->
    gen_server:start_link({local, line_server}, ?MODULE, [Path], []).

%% MFA Server - load based on MFA
start_mfa(LoadMFA, ReadMFA) ->
    Args = [{mfa, LoadMFA, ReadMFA}],
    gen_server:start_link({local, mfa_server}, ?MODULE, Args, []).


%% ====================================================================
%% Callbacks
%% ====================================================================

%% -spec init([string(), [binary()], string()]) -> {ok, #state{}}.
init([{mfa, {M, F, A}, ReadMFA}]) ->
    Data = apply(M, F, A),
    {ok, #state{cache=Data, read_mfa=ReadMFA}};
init([Path, Fields, SchemaPath]) ->
    {ok, F} = file:open(Path, [read, raw, binary, {read_ahead, ?MB}]),
    Schema = load_schema(SchemaPath),
    {ok, #state{cache=[], fields=Fields, file=F, schema=Schema}};
init([Path]) ->
    {ok, F} = file:open(Path, [read, raw, binary, {read_ahead, ?MB}]),
    {ok, #state{cache=[], i=1, file=F}}.

handle_call(_, _, S=#state{finished=true}) ->
    {reply, finished, S};

handle_call({read_mfa, Op}, _From, S) ->
    Cache = S#state.cache,
    {M, F, A} = S#state.read_mfa,
    case apply(M, F, [Op,Cache|A]) of
        {KeyValue, Cache2} ->
            {reply, KeyValue, S#state{cache=Cache2}};
        finished ->
            %% Rather than stop the server stay up so that all workers
            %% can see 'finished' state and shutdown properly.
            {reply, finished, S#state{finished=true}}
    end;

handle_call({ft, N}, _From, S=#state{cache=[],
                                     fields=Fields,
                                     file=F,
                                     schema=Schema}) ->
    Pairs = read_pairs(F),
    Pairs2 = lists:filter(match(Fields), Pairs),
    Pairs3 = lists:map(analyze(Schema), Pairs2),
    {Pair, Cache2} = get_terms(Pairs3, N),
    {reply, Pair, S#state{cache=Cache2}};
handle_call({ft, N}, _From, S=#state{cache=Cache}) ->
    {Pair, Cache2} = get_terms(Cache, N),
    {reply, Pair, S#state{cache=Cache2}};
handle_call(line, _From, S=#state{cache=[], i=I, file=F}) ->
    %% TODO no wrap around on line?
    {[Line], Cache} = lists:split(1, read_lines(F, 10)),
    Is = integer_to_list(I),
    {reply, {Is, Line}, S#state{cache=Cache, i=I+1}};
handle_call(line, _From, S=#state{cache=[Line|Cache], i=I}) ->
    Is = integer_to_list(I),
    {reply, {Is, Line}, S#state{cache=Cache, i=I+1}}.

terminate(_Reason, _State) -> ignore.

%% ====================================================================
%% Private
%% ====================================================================

get_terms([{Field, Terms}|Pairs], N) ->
    {T1, T2} = lists:split(N, Terms),
    Pair = {Field, T1},
    case length(T2) of
        0 -> {Pair, Pairs};
        _ -> {Pair, [{Field, T2}|Pairs]}
    end.

match(Fields) ->
    S = sets:from_list(Fields),
    fun({Field, _}) ->
            sets:is_element(Field, S)
    end.

analyze(Schema) ->
    fun({Field, Val}) ->
            X = Schema:find_field(Field),
            {erlang, M, F} = Schema:analyzer_factory(X),
            A = Schema:analyzer_args(X),
            {ok, Res} = M:F(Val, A),
            {Field, Res}
    end.

load_schema(Path) ->
    {ok, S1} = file:read_file(Path),
    {ok, S2} = riak_search_utils:consult(S1),
    {ok, S3} = riak_search_schema_parser:from_eterm(<<"test">>, S2),
    S3.

read_pairs(F) ->
    read_pairs(F, 0).

read_pairs(_F, 100) ->
    %% Guard against infinite loop
    throw({field_extraction, too_many_retries});

read_pairs(F, Retry) ->
    Line = read_line(F),

    %% Guard against invalid JSON
    try
        riak_search_kv_json_extractor:extract_value(Line, default_field,
                                                    ignored)
    catch _:Reason ->
            ?ERROR("Failed to extract line: ~p", [Reason]),
            read_pairs(F, Retry+1)
    end.

read_lines(F, N) -> read_lines(F, N, []).

read_lines(_F, 0, Lines) -> lists:reverse(Lines);
read_lines(F, N, Lines) ->
    Line = read_line(F),
    read_lines(F, N-1, [Line|Lines]).

read_line(F) ->
    case file:read_line(F) of
        {ok, Line} -> ok;
        eof ->
            file:position(F, bof),
            {ok, Line} = file:read_line(F),
            ok
    end,
    Line.
