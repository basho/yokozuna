EXOMETER_PACKAGES = "(basic)"
export EXOMETER_PACKAGES
PULSE_TESTS = yz_solrq_eqc

REBAR ?= $(shell pwd)/rebar3

.PHONY: deps rel stagedevrel test

all: deps compile-riak-test

compile: deps
	$(REBAR) compile

compile-riak-test: compile
#	I'm truly sorry
	mkdir -p riak_test/ebin
	erlc +nowarn_export_all -oriak_test/ebin -Iinclude -I_build/default/lib \
		-pz _build/default/lib/yokozuna/ebin riak_test/*.erl

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean
	rm -rf riak_test/ebin
	rm -rf _build
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
RIAK_TEST_PATH = riak_test

include tools.mk

${TEST_PLT}: compile-riak-test
	@if [ -d $(RIAK_TEST_PATH) ]; then \
		if [ -f $(TEST_PLT) ]; then \
			dialyzer --check_plt --plt $(TEST_PLT) $(RIAK_TEST_PATH)/ebin && \
			dialyzer --add_to_plt --plt $(TEST_PLT) --apps edoc --output_plt $(TEST_PLT) ebin $(RIAK_TEST_PATH)/ebin ; test $$? -ne 1; \
		else \
			dialyzer --build_plt --apps edoc --output_plt $(TEST_PLT) ebin $(RIAK_TEST_PATH)/ebin ; test $$? -ne 1; \
		fi \
	fi

dialyzer-rt-run:
	@echo "==> $(shell basename $(shell pwd)) (dialyzer_rt)"
	@PLTS="$(PLT) $(LOCAL_PLT) $(TEST_PLT)"; \
	if [ -f dialyzer.ignore-warnings ]; then \
		if [ $$(grep -cvE '[^[:space:]]' dialyzer.ignore-warnings) -ne 0 ]; then \
			echo "ERROR: dialyzer.ignore-warnings contains a blank/empty line, this will match all messages!"; \
			exit 1; \
		fi; \
		dialyzer $(DIALYZER_FLAGS) --plts $${PLTS} -c $(RIAK_TEST_PATH)/ebin > dialyzer_warnings ; \
		cat dialyzer.ignore-warnings \
		| sed -E 's/^([^:]+:)[^:]+:/\1/' \
		| sort \
		| uniq -c \
		| sed -E '/.*\.erl: /!s/^[[:space:]]*[0-9]+[[:space:]]*//' \
		> dialyzer.ignore-warnings.tmp ; \
		egrep -v "^[[:space:]]*(done|Checking|Proceeding|Compiling)" dialyzer_warnings \
		| sed -E 's/^([^:]+:)[^:]+:/\1/' \
		| sort \
		| uniq -c \
		| sed -E '/.*\.erl: /!s/^[[:space:]]*[0-9]+[[:space:]]*//' \
		| grep -F -f dialyzer.ignore-warnings.tmp -v \
		| sed -E 's/^[[:space:]]*[0-9]+[[:space:]]*//' \
		| sed -E 's/([]\^:+?|()*.$${}\[])/\\\1/g' \
		| sed -E 's/(\\\.erl\\\:)/\1[[:digit:]]+:/g' \
		| sed -E 's/^(.*)$$/^[[:space:]]*\1$$/g' \
		> dialyzer_unhandled_warnings ; \
		rm dialyzer.ignore-warnings.tmp; \
		if [ $$(cat dialyzer_unhandled_warnings | wc -l) -gt 0 ]; then \
		    egrep -f dialyzer_unhandled_warnings dialyzer_warnings ; \
			found_warnings=1; \
	    fi; \
		[ "$$found_warnings" != 1 ] ; \
	else \
		dialyzer -Wno_return $(DIALYZER_FLAGS) --plts $${PLTS} -c $(RIAK_TEST_PATH)/ebin; \
	fi

dialyzer_rt: deps ${PLT} ${LOCAL_PLT} $(TEST_PLT) dialyzer-rt-run

##
## Purity
##
## NOTE: Must add purity to ERL_LIBS for these targets to work
build_purity_plt:
	@erl -noshell -run purity_cli main -extra --build-plt --apps $(APPS) deps/*/ebin ebin

purity:
	@erl -noshell -run purity_cli main -extra -v -s stats --with-reasons -l 3 --apps ebin
