EXOMETER_PACKAGES = "(basic)"
export EXOMETER_PACKAGES

REBAR ?= $(shell pwd)/rebar

.PHONY: deps rel stagedevrel test

all: deps compile-riak-test

compile: deps
	$(REBAR) compile

compile-riak-test: compile
	$(REBAR) skip_deps=true riak_test_compile

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean
	rm -rf riak_test/ebin
	rm -rf build
	git clean -dfx priv/

distclean: clean
	$(REBAR) delete-deps

##
## Dialyzer
##
DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool snmp public_key mnesia eunit syntax_tools compiler
DIALYZER_FLAGS = -Wno_return
TEST_PLT = .yokozuna_test_dialyzer_plt

include tools.mk

${TEST_PLT}: compile-riak-test
ifneq (,$(RIAK_TEST_PATH))
ifneq (,$(wildcard $(TEST_PLT)))
	dialyzer --check_plt --plt $(TEST_PLT) && \
		dialyzer --add_to_plt --plt $(TEST_PLT) --apps edoc --output_plt $(TEST_PLT) ebin $(RIAK_TEST_PATH)/ebin $(RIAK_TEST_PATH)/deps/riakc/ebin ; test $$? -ne 1
else
	dialyzer --build_plt --apps edoc --output_plt $(TEST_PLT) ebin $(RIAK_TEST_PATH)/ebin $(RIAK_TEST_PATH)/deps/riakc ; test $$? -ne 1
endif
else
	@echo "Set RIAK_TEST_PATH"
	exit 1
endif

dialyzer_rt: deps ${PLT} ${LOCAL_PLT} $(TEST_PLT)
	dialyzer -Wno_return --plts $(PLT) $(LOCAL_PLT) $(TEST_PLT) -c riak_test/ebin

##
## Purity
##
## NOTE: Must add purity to ERL_LIBS for these targets to work
build_purity_plt:
	@erl -noshell -run purity_cli main -extra --build-plt --apps $(APPS) deps/*/ebin ebin

purity:
	@erl -noshell -run purity_cli main -extra -v -s stats --with-reasons -l 3 --apps ebin
