-module(yz_doc).
-compile(export_all).
-include("yokozuna.hrl").

%% @doc Functionality for working with Yokozuna documents.

%%%===================================================================
%%% API
%%%===================================================================

add_to_doc({doc, Fields}, Field) ->
    {doc, [Field|Fields]}.

%% @doc Given an object generate the doc to be indexed by Solr.
-spec make_doc(riak_object:riak_object()) -> doc().
make_doc(O) ->
    %% TODO: For now assume text/plain to prototype
    %%
    %% TODO: change 'text' to 'value'
    Fields = [{id, doc_id(O)},
              {text, value(O)},
              {'_vc', gen_vc(O)}],
    {doc, Fields}.

%%%===================================================================
%%% Private
%%%===================================================================

doc_id(O) ->
    riak_object:key(O).

%% TODO: Just pass metadata in?
%%
%% TODO: I don't like having X-Riak-Last-Modified in here.  Add
%%       function to riak_object.
doc_ts(O) ->
    MD = riak_object:get_metadata(O),
    dict:fetch(<<"X-Riak-Last-Modified">>, MD).

doc_vclock(O) ->
    riak_object:vclock(O).

gen_ts() ->
    {{Year, Month, Day},
     {Hour, Min, Sec}} = calendar:now_to_universal_time(erlang:now()),
    list_to_binary(io_lib:format("~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0B",
                                 [Year,Month,Day,Hour,Min,Sec])).

gen_vc(O) ->
    TS = gen_ts(),
    ID = doc_id(O),
    VClock = base64:encode(crypto:sha(term_to_binary(doc_vclock(O)))),
    <<TS/binary," ",ID/binary," ",VClock/binary>>.

value(O) ->
    riak_object:get_value(O).
