%% @doc Test the startup and shutdown sequence.
-module(yz_startup_shutdown).
-export([confirm/0]).
-include("yokozuna.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile({parse_transform, rt_intercept_pt}).

-define(CONFIG, [{yokozuna, [{enabled, true}]}]).

confirm() ->
    Cluster = rt:build_cluster(2, ?CONFIG),
    rt:wait_for_cluster_service(Cluster, yokozuna),

    %% compile(rt_intercept_pt, Cluster, [{parse_transform, rt_intercept_pt}]),
    verify_yz_components_enabled(Cluster),

    intercept_yz_solrq_sup_drain(Cluster),
    stop_yokozuna(Cluster),

    verify_drain_called(Cluster),
    %% assert yz_pb_search service is deregistered?
    %% assert yz_pb_admin service is deregistered?
    verify_yz_components_disabled(Cluster),
    pass.

verify_yz_components_enabled(Cluster) ->
    check_yz_components(Cluster, true).

verify_yz_components_disabled(Cluster) ->
    check_yz_components(Cluster, false).

check_yz_components([], _) ->
    ok;
check_yz_components([Node|Rest], Enabled) ->
    Components = yz_app:components(),
    lists:all(
      fun(Component) ->
              Enabled =:= rpc:call(Node, yokozuna, is_enabled, [Component])
      end,
      Components),
    check_yz_components(Rest, Enabled).

intercept_yz_solrq_sup_drain([]) ->
    ok;
intercept_yz_solrq_sup_drain([Node|Rest]) ->
    RiakTestProcess = self(),
    rt_intercept:add(
      Node,
      {yz_solrq_sup,
       [{{drain, 0},
         {[Node, RiakTestProcess],
          fun() ->
                  RiakTestProcess ! {Node, drain_called}
          end}}]}),
    intercept_yz_solrq_sup_drain(Rest).

stop_yokozuna([]) ->
    ok;
stop_yokozuna([Node|Rest]) ->
    ok = rpc:call(Node, application, stop, [yokozuna]),
    stop_yokozuna(Rest).

verify_drain_called(Cluster) ->
    Results = [begin
                   receive
                       {Node, drain_called} ->
                           ok
                   after
                       10000 ->
                           {fail, timeout}
                   end
               end
               || Node <- Cluster],

    true = lists:all(fun(Result) ->
                             ok =:= Result
                     end,
                     Results).

%% compile(Module, Cluster) ->
%%     compile(Module, Cluster, []).
%%
%% compile(Module, Cluster, UserOptions) ->
%%     %% Trigger load if not present
%%     _ = Module:module_info(),
%%
%%     ?INFO("Hello Fuckers ~p", [proplists:get_value(Module, code:all_loaded())]),
%%     %% Then work out where we loaded it from
%%     case proplists:get_value(Module, code:all_loaded()) of
%%         undefined ->
%%             {error, not_loaded};
%%         BeamName ->
%%             do_compile_beam(Module, BeamName, Cluster, UserOptions)
%%     end.
%%
%% %% Beam is a binary or a .beam file name
%% do_compile_beam(Module, Beam, Cluster, UserOptions) ->
%%     ?INFO("Abstract code: ~p", [get_abstract_code(Module, Beam)]),
%%     case get_abstract_code(Module, Beam) of
%%         no_abstract_code=E ->
%%             {error,E};
%%         encrypted_abstract_code=E ->
%%             {error,E};
%%         {_Vsn,Code} ->
%%             Forms0 = epp:interpret_file_attribute(Code),
%%             Forms = Module:parse_transform(Forms0, UserOptions),
%%
%%             %% We need to recover the source from the compilation
%%             %% info otherwise the newly compiled module will have
%%             %% source pointing to the current directory
%%             SourceInfo = get_source_info(Module, Beam),
%%
%%             %% Compile and load the result
%%             %% It's necessary to check the result of loading since it may
%%             %% fail, for example if Module resides in a sticky directory
%%             {ok, Module, Binary} = compile:forms(Forms, SourceInfo ++ UserOptions),
%%             ?INFO("load_binary: ~p", [code:load_binary(Module, Beam, Binary)]),
%%             [begin
%%                  case rpc:call(Node, code, load_binary, [Module, Beam, Binary]) of
%%                      {module, Module} ->
%%                          {ok, Module};
%%                      Error ->
%%                          Error
%%                  end
%%              end || Node <- Cluster]
%%     end.
%%
%% get_abstract_code(Module, Beam) ->
%%     case beam_lib:chunks(Beam, [abstract_code]) of
%%         {ok, {Module, [{abstract_code, AbstractCode}]}} ->
%%             AbstractCode;
%%         {error,beam_lib,{key_missing_or_invalid,_,_}} ->
%%             encrypted_abstract_code;
%%         Error -> Error
%%     end.
%%
%% get_source_info(Module, Beam) ->
%%     case beam_lib:chunks(Beam, [compile_info]) of
%%         {ok, {Module, [{compile_info, Compile}]}} ->
%%             case lists:keyfind(source, 1, Compile) of
%%                 { source, _ } = Tuple -> [Tuple];
%%                 false -> []
%%             end;
%%         _ ->
%%             []
%%     end.
