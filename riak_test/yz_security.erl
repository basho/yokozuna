%% @doc Ensure that only approved users can search,index,schema
%%      if security is activated
-module(yz_security).
-compile(export_all).
-import(yz_rt, [host_entries/1,
                run_bb/2, search_expect/5,
                select_random/1, verify_count/2,
                write_terms/2]).
-import(rt, [connection_info/1,
             build_cluster/2, wait_for_cluster_service/2]).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ROOT_CERT, "certs/selfsigned/ca/rootcert.pem").
-define(CFG(PrivDir),
        [{riak_core, [
           {ring_creation_size, 16},
           {default_bucket_props, [{allow_mult, true}]},
           {ssl, [
             {certfile, filename:join([PrivDir,
                                       "certs/selfsigned/site3-cert.pem"])},
             {keyfile, filename:join([PrivDir,
                                       "certs/selfsigned/site3-key.pem"])}
              ]},
           {security, true}
          ]},
         {riak_api, [
             {certfile, filename:join([PrivDir,
                                       "certs/selfsigned/site3-cert.pem"])},
             {keyfile, filename:join([PrivDir,
                                       "certs/selfsigned/site3-key.pem"])},
             {cacertfile, filename:join([PrivDir, ?ROOT_CERT])}
          ]},
         {yokozuna, [
           {enabled, true}
          ]}
        ]).
-define(USER, "user").
-define(PASSWORD, "password").

confirm() ->
    application:start(crypto),
    application:start(asn1),
    application:start(public_key),
    application:start(ssl),
    application:start(ibrowse),
    % lager:info("turning on tracing"),
    % ibrowse:trace_on(),
    % YZBenchDir = rt_config:get_os_env("YZ_BENCH_DIR"),
    % code:add_path(filename:join([YZBenchDir, "ebin"])),
    % random:seed(now()),
    % this is a similar duplicate riak_test fix as rt:priv_dir
    PrivDir = re:replace(code:priv_dir(riak_test),
        "riak_test(/riak_test)*", "riak_test", [{return, list}]),
    % PrivDir = "/Users/ericredmond/tmp/yz-verify/riak_test/priv",
    lager:info("PrivDir: ~p", [PrivDir]),
    Cluster = build_cluster(1, ?CFG(PrivDir)),
    Node = hd(Cluster),
    enable_https(Node),
    wait_for_cluster_service(Cluster, yokozuna),
    create_user(Node, PrivDir),
    pass.


enable_https(Node) ->
    [{http, {_IP, Port}}|_] = connection_info(Node),
    HP = {"127.0.0.1", Port+1000},
    rt:update_app_config(Node, [{riak_api, [{https, [HP]}]}]),
    ok.

create_user(Node, PrivDir) ->
    [_, {pb, {"127.0.0.1", Port}}] = connection_info(Node),

    lager:info("Creating user"),
    ok = rpc:call(Node, riak_core_console, add_user,
                  [[?USER, "password="++?PASSWORD]]),

    lager:info("Setting password mode on user"),
    % ok = rpc:call(Node, riak_core_console, add_source, [[?USER, "127.0.0.1/32",
    %                                                      ?PASSWORD]]),
    ok = rpc:call(Node, riak_core_console, add_source, [[?USER, "127.0.0.1/32",
                                                    "trust"]]),

    Cacertfile = filename:join([PrivDir, ?ROOT_CERT]),
    % lager:info("Cacertfile: ~p", [Cacertfile]),
    {ok, PB} = riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, ?USER, ?PASSWORD},
                                       {cacertfile, Cacertfile}]),
    ?assertEqual(pong, riakc_pb_socket:ping(PB)),

    ok.


% %% @doc Test yokozuna secuity for search, index and schema
% -module(yz_security).
% -compile(export_all).
% -include_lib("eunit/include/eunit.hrl").

% -define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
% -define(NO_HEADERS, []).
% -define(NO_BODY, <<>>).
% %% certs are provided by riak_test
% -define(CFG(PrivDir),
%         [
%          {riak_core,
%           [
%            {ring_creation_size, 8},
%            {default_bucket_props, [{allow_mult, true}]},
%             {ssl, [
%                     {certfile, filename:join([PrivDir,
%                                               "certs/selfsigned/site3-cert.pem"])},
%                     {keyfile, filename:join([PrivDir,
%                                              "certs/selfsigned/site3-key.pem"])},
%                     {cacertfile, filename:join([PrivDir,
%                                                "certs/selfsigned/ca/rootcert.pem"])}
%                     ]},
%             {security, true}
%           ]},
%          {yokozuna,
%           [
%            {enabled, true}
%           ]}
%         ]).
% -define(USER, "user").
% -define(BUCKET, "testbucket").
% -define(INDEX, "testindex").

% confirm() ->
%     YZBenchDir = rt_config:get_os_env("YZ_BENCH_DIR"),
%     code:add_path(filename:join([YZBenchDir, "ebin"])),
%     random:seed(now()),
%     Cluster = rt:build_cluster(1, ?CFG(rt:priv_dir())),
%     % rt:wait_for_cluster_service(Cluster, yokozuna),
%     Node = hd(Cluster),
%     enable_ssl(Node),

%     % {ok, [{"127.0.0.1", Port0}]} = rpc:call(Node, application, get_env,
%     %                              [riak_api, http]),
%     {ok, [{"127.0.0.1", Port}]} = rpc:call(Node, application, get_env,
%                                  [riak_api, https]),

%     setup_user(Node),

%     confirm_reject_invalid_user(Node, Port),
%     confirm_search_specific_index(Node),
%     pass.

% enable_ssl(Node) ->
%     [{http, {_IP, Port}}|_] = rt:connection_info(Node),
%     rt:update_app_config(Node, [{riak_api, [{https, [{"127.0.0.1",
%                                                      Port+1000}]}]}]),
%     rt:wait_until_pingable(Node),
%     rt:wait_for_service(Node, yokozuna).

% setup_user(Node) ->
%     %% grant the user credentials
%     ok = rpc:call(Node, riak_core_console, add_user, [[?USER, "password=password"]]),
%     %% require password on localhost
%     ok = rpc:call(Node, riak_core_console, add_source, [[?USER,
%                                                          "127.0.0.1/32",
%                                                          "password"]]).

% confirm_reject_invalid_user(Node, Port) ->
%     %% invalid credentials should be rejected in password mode
%     C4 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
%                                                                 ?USER,
%                                                                  "badpass"}]),
%     ?assertMatch({error, {ok, "401", _, _}}, rhc:ping(C4)),

%     %% valid credentials should be accepted in password mode
%     C5 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
%                                                                 ?USER,
%                                                                  "password"}]),

%     ?assertEqual(ok, rhc:ping(C5)),

%     %% setup a user who can put/get
%     ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.put", "ON",
%                                                 "default", ?BUCKET, "TO", ?USER]]),
%     ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get", "ON",
%                                                 "default", ?BUCKET, "TO", ?USER]]),

%     ok.

% confirm_search_specific_index(Node) ->
%     ok = rpc:call(Node, riak_core_console, grant, [["yokozuna.search", "ON",
%                                                 "index", ?INDEX, "TO", ?USER]]),

%     % lager:info("verifying that user cannot get/put without grants"),
%     % ?assertMatch({error, {ok, "403", _, _}}, rhc:get(C7, <<"hello">>,
%     %                                                  <<"world">>)),

%     % Object = riakc_obj:new(<<"hello">>, <<"world">>, <<"howareyou">>,
%     %                        <<"text/plain">>),

%     % ?assertMatch({error, {ok, "403", _, _}}, rhc:put(C7, Object)),

%     ok.
