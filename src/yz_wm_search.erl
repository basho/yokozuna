-module(yz_wm_search).
-compile(export_all).
-include_lib("webmachine/include/webmachine.hrl").

init(_) ->
    {ok, none}.

allowed_methods(Req, S) ->
    Methods = ['GET'],
    {Methods, Req, S}.

content_types_provided(Req, S) ->
    Types = [{"text/xml", search}],
    {Types, Req, S}.

search(Req, S) ->
    Index = wrq:path_info(index, Req),
    %% Query = wrq:get_qs_value("q", Req),
    Params = wrq:req_qs(Req),
    Mapping = yz_events:get_mapping(),
    XML = yz_solr:search(Index, Params, Mapping),
    Req2 = wrq:set_resp_header("Content-Type", "text/xml", Req),
    {XML, Req2, S}.
