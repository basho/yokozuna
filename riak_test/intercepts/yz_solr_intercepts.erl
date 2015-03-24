-module(yz_solr_intercepts).
-compile(export_all).

slow_cores() ->
    timer:sleep(6000),
    {ok, []}.
