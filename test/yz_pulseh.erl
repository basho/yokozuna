-module(yz_pulseh).
-compile([export_all]).

compile(Module) ->
    compile(Module, []).

compile(Module, UserOptions) ->
    %% Trigger load if not present
    _ = Module:module_info(),

    %% Then work out where we loaded it from
    case proplists:get_value(Module, code:all_loaded()) of
        undefined ->
            {error, not_loaded};
        [$p, $u, $l, $s, $e, $d, $_ |  BeamName] ->
            do_compile_beam(Module, BeamName, UserOptions);
        BeamName ->
            do_compile_beam(Module, BeamName, UserOptions)
    end.

%% Beam is a binary or a .beam file name
do_compile_beam(Module,Beam,UserOptions) ->
    %% Extract the abstract format and apply the pulse instrument
    %% every executable line and, as a side effect, initiate
    %% the database
    
    case get_abstract_code(Module, Beam) of
	no_abstract_code=E ->
	    {error,E};
	encrypted_abstract_code=E ->
	    {error,E};
	{_Vsn,Code} ->
            Forms0 = epp:interpret_file_attribute(Code),
	    Forms = pulse_instrument:parse_transform(Forms0, UserOptions),

	    %% We need to recover the source from the compilation
	    %% info otherwise the newly compiled module will have
	    %% source pointing to the current directory
	    SourceInfo = get_source_info(Module, Beam),

	    %% Compile and load the result
	    %% It's necessary to check the result of loading since it may
	    %% fail, for example if Module resides in a sticky directory
	    {ok, Module, Binary} = compile:forms(Forms, SourceInfo ++ UserOptions),
	    case code:load_binary(Module, "pulsed_" ++ Beam, Binary) of
		{module, Module} ->
		    {ok, Module};
		Error ->
                    Error
	    end
    end.
get_abstract_code(Module, Beam) ->
    case beam_lib:chunks(Beam, [abstract_code]) of
	{ok, {Module, [{abstract_code, AbstractCode}]}} ->
	    AbstractCode;
	{error,beam_lib,{key_missing_or_invalid,_,_}} ->
	    encrypted_abstract_code;
	Error -> Error
    end.

get_source_info(Module, Beam) ->
    case beam_lib:chunks(Beam, [compile_info]) of
	{ok, {Module, [{compile_info, Compile}]}} ->
		case lists:keyfind(source, 1, Compile) of
			{ source, _ } = Tuple -> [Tuple];
			false -> []
		end;
	_ ->
		[]
    end.
