-module(yokozuna).
-include("yokozuna.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         index/1,
         ping/0
        ]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Index the given object `O'.
-spec index(riak_object:riak_object()) -> ok | {error, term()}.
index(O) ->
    esolr:add([make_doc(O)]).

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, yokozuna),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, yokozuna_vnode_master).


%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Given an object generate the doc to be indexed by Solr.
-spec make_doc(riak_object:riak_object()) -> doc().
make_doc(O) ->
    %% TODO: For now assume text/plain to prototype
    Fields = [{id, doc_id(O)},
              {value, value(O)}],
    {doc, Fields}.

doc_id(O) ->
    riak_object:key(O).

value(O) ->
    riak_object:get_value(O).
