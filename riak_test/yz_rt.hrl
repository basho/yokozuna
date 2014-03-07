-type cluster() :: [node()].
-type conns() :: [pid()].
-type cluster_and_conns() :: {cluster(), conns()}.
-type host() :: string().
-type mode() :: async | sync.
-type portnum() :: integer().

%% Copied from rt.erl, would be nice if there was a rt.hrl
-type interface() :: {http, tuple()} | {pb, tuple()}.
-type interfaces() :: [interface()].
-type conn_info() :: [{node(), interfaces()}].
-type prop() :: {atom(), any()}.
-type props() :: [prop()].


