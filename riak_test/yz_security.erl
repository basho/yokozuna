%% @doc Ensure that only approved users can search,index,schema
%%      if security is activated
-module(yz_security).
-compile(export_all).
-import(yz_rt, [host_entries/1]).
-import(rt, [connection_info/1,
             build_cluster/2, wait_for_cluster_service/2]).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CERT_FILE(CertDir, CertName), CertDir ++ "/" ++ CertName ++ "/cert.pem").
-define(KEY_FILE(CertDir, CertName), CertDir ++ "/" ++ CertName ++ "/key.pem").
-define(ROOT_CERT(CertDir), ?CERT_FILE(CertDir, "rootCA")).
-define(CFG(PrivDir, CertDir),
        [{riak_core, [
           {ring_creation_size, 16},
           {default_bucket_props, [{allow_mult, true}]},
           {ssl, [
             {certfile, ?CERT_FILE(CertDir, "site3.basho.com")},
             {keyfile, ?KEY_FILE(CertDir, "site3.basho.com")}
           ]}
          ]},
         {riak_api, [
             {certfile, ?CERT_FILE(CertDir, "site3.basho.com")},
             {keyfile, ?KEY_FILE(CertDir, "site3.basho.com")},
             {cacertfile, ?ROOT_CERT(CertDir)}
          ]},
         {yokozuna, [
           {enabled, true}
          ]}
        ]).
-define(SCHEMA_CONTENT, <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?>
<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_err\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
</fields>
<uniqueKey>_yz_id</uniqueKey>
<types>
    <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
</types>
</schema>">>).
-define(USER, "user").
-define(PASSWORD, "password").
-define(INDEX, <<"myindex">>).
-define(INDEX2, <<"myindex2">>).
-define(SCHEMA, <<"myschema">>).
-define(HUSER, "h_user").
-define(HINDEX, "h_myindex").
-define(ADD_USER(N,D), rpc:call(N, riak_core_console, add_user, D)).
-define(ADD_SOURCE(N,D), rpc:call(N, riak_core_console, add_source, D)).
-define(GRANT(N,D), rpc:call(N, riak_core_console, grant, D)).

confirm() ->
    application:start(crypto),
    application:start(asn1),
    application:start(public_key),
    application:start(ssl),
    application:start(ibrowse),
    PrivDir = rt:priv_dir(),
    lager:info("r_t priv: ~p", [PrivDir]),
    CertDir = get_cert_dir(),
    make_certs:rootCA(CertDir, "rootCA"),
    make_certs:endusers(CertDir, "rootCA", ["site3.basho.com"]),
    Cluster = build_cluster(1, ?CFG(PrivDir, CertDir)),
    Node = hd(Cluster),
    %% enable security on the cluster
    ok = rpc:call(Node, riak_core_console, security_enable, [[]]),
    enable_https(Node),
    wait_for_cluster_service(Cluster, yokozuna),
    create_user(Node, ?USER),
    confirm_index_pb(Node),
    confirm_search_pb(Node),
    confirm_schema_permission_pb(Node),
    create_user(Node, ?HUSER),
    confirm_index_https(Node),
    pass.

enable_https(Node) ->
    {Host, Port} = proplists:get_value(http, connection_info(Node)),
    rt:update_app_config(Node, [{riak_api, [{https, [{Host, Port+1000}]}]}]),
    ok.

get_cert_dir() ->
    rt_config:get(rt_scratch_dir) ++ "/certs".

get_secure_pid(Host, Port) ->
    Cacertfile = ?ROOT_CERT(get_cert_dir()),
    {ok, Pid} = riakc_pb_socket:start(Host, Port,
                                      [{credentials, ?USER, ?PASSWORD},
                                       {cacertfile, Cacertfile}]),
    Pid.

create_user(Node, User) ->
    {Host, Port} = proplists:get_value(pb, connection_info(Node)),

    {ok, PB0} =  riakc_pb_socket:start(Host, Port, []),
    ?assertEqual({error, <<"Security is enabled, please STARTTLS first">>},
                 riakc_pb_socket:ping(PB0)),

    lager:info("Adding a user"),
    ok = ?ADD_USER(Node, [[User, "password="++?PASSWORD]]),

    lager:info("Setting password mode on user"),
    ok = ?ADD_SOURCE(Node, [[User, Host++"/32", ?PASSWORD]]),

    Pid = get_secure_pid(Host, Port),
    ?assertEqual(pong, riakc_pb_socket:ping(Pid)),
    riakc_pb_socket:stop(Pid),
    ok.

confirm_index_pb(Node) ->
    {Host, Port} = proplists:get_value(pb, connection_info(Node)),

    Pid0 = get_secure_pid(Host, Port),
    lager:info("verifying user cannot create index without grants"),
    ?assertMatch({error, <<"Permission", _/binary>>},
        riakc_pb_socket:create_search_index(Pid0, ?INDEX)),

    lager:info("verifying user cannot list indexes without grants"),
    ?assertMatch({error, <<"Permission", _/binary>>},
        riakc_pb_socket:list_search_indexes(Pid0)),

    riakc_pb_socket:stop(Pid0),

    lager:info("Grant index permission to user"),
    ok = ?GRANT(Node, [[?YZ_SECURITY_ADMIN_PERM,"on","index","to",?USER]]),

    Pid1 = get_secure_pid(Host, Port),
    lager:info("verifying user can create an index"),
    ?assertEqual(ok, riakc_pb_socket:create_search_index(Pid1, ?INDEX)),

    %% create another index, never give permission to use it
    ?assertEqual(ok,
        riakc_pb_socket:create_search_index(Pid1, ?INDEX2)),

    ?assertEqual(ok,
        riakc_pb_socket:create_search_index(Pid1, <<"_gonna_be_dead_">>)),

    lager:info("verifying user can delete an index"),
    ?assertEqual(ok,
        riakc_pb_socket:delete_search_index(Pid1, <<"_gonna_be_dead_">>)),

    lager:info("verifying user can get an index"),
    ?assertMatch({ok,[_|_]},
        riakc_pb_socket:get_search_index(Pid1, ?INDEX)),

    lager:info("verifying user can get all indexes"),
    ?assertMatch({ok,[_|_]},
        riakc_pb_socket:list_search_indexes(Pid1)),
    riakc_pb_socket:stop(Pid1),
    ok.

confirm_schema_permission_pb(Node) ->
    {Host, Port} = proplists:get_value(pb, connection_info(Node)),

    Pid0 = get_secure_pid(Host, Port),
    lager:info("verifying user cannot create schema without grants"),
    ?assertMatch({error, <<"Permission", _/binary>>},
        riakc_pb_socket:create_search_schema(Pid0, ?SCHEMA, ?SCHEMA_CONTENT)),

    lager:info("verifying user cannot get schemas without grants"),
    ?assertMatch({error, <<"Permission", _/binary>>},
        riakc_pb_socket:get_search_schema(Pid0, ?SCHEMA)),
    riakc_pb_socket:stop(Pid0),

    lager:info("Grant schema permission to user"),
    ok = ?GRANT(Node, [[?YZ_SECURITY_ADMIN_PERM,"on","schema","to",?USER]]),

    Pid1 = get_secure_pid(Host, Port),
    lager:info("verifying user can create schema"),
    ?assertMatch(ok,
        riakc_pb_socket:create_search_schema(Pid1, ?SCHEMA, ?SCHEMA_CONTENT)),
    riakc_pb_socket:stop(Pid1),
    ok.

confirm_search_pb(Node) ->
    {Host, Port} = proplists:get_value(pb, connection_info(Node)),

    Pid0 = get_secure_pid(Host, Port),
    lager:info("verifying user cannot search an index without grants"),
    ?assertMatch({error, <<"Permission", _/binary>>},
        riakc_pb_socket:search(Pid0, ?INDEX, <<"*:*">>)),
    riakc_pb_socket:stop(Pid0),

    lager:info("Grant search permission to user on ~s", [?INDEX]),
    IndexS = binary_to_list(?INDEX),
    ok = ?GRANT(Node, [[?YZ_SECURITY_SEARCH_PERM,"on","index",IndexS,"to",?USER]]),

    Pid1 = get_secure_pid(Host, Port),
    lager:info("verifying user can search granted on ~s", [?INDEX]),
    ?assertMatch({ok, _Result},
        riakc_pb_socket:search(Pid1, ?INDEX, <<"*:*">>)),

    lager:info("verifying user cannot search a different index"),
    ?assertMatch({error, <<"Permission", _/binary>>},
        riakc_pb_socket:search(Pid1, ?INDEX2, <<"*:*">>)),

    riakc_pb_socket:stop(Pid1),
    ok.


get_auth_header(User, Password) ->
    Token = base64:encode_to_string(User ++ ":" ++ Password),
    [{"Authorization", "Basic " ++ Token}].

get_ssl_options(Options) ->
    [{is_ssl, true}, {ssl_options, Options}].

confirm_index_https(Node) ->
    {Host, Port} = proplists:get_value(https, connection_info(Node)),

    lager:info("verifying the peer certificate should work if the cert is valid"),

    Cacertfile = ?ROOT_CERT(get_cert_dir()),

    lager:info("Cacertfile: ~p", [Cacertfile]),
    Opts = [{is_ssl, true}, {ssl_options, [
             {cacertfile, Cacertfile},
             {verify, verify_peer},
             {server_name_indication, disable},
             {reuse_sessions, false}
            ]}],
    Headers = [{"accept", "multipart/mixed, */*;q=0.9"}] ++
              [{"content-type", "application/json"}]
               ++ get_auth_header(?HUSER, ?PASSWORD),
    Body = <<"{\"schema\":\"_yz_default\"}">>,
    URL = lists:flatten(io_lib:format("https://~s:~s/search/index/~s",
                                      [Host, integer_to_list(Port), ?HINDEX])),
    {ok, "403", _, _} = ibrowse:send_req(URL, Headers, put, Body, Opts),

    lager:info("Grant index permission to user"),
    ok = ?GRANT(Node, [[?YZ_SECURITY_ADMIN_PERM,"on","index","to",?HUSER]]),

    %% this should work now that this user has permission
    {ok, "204", _, _} = ibrowse:send_req(URL, Headers, put, Body, Opts),
    ok.
