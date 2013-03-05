#!/bin/bash
#
# Verify that Yokozuna...
#
# 1. compiles
# 2. dialyzes
# 3. passes unit tests
# 4. passes riak_test tests
#
# Yes, this script is sloppy, very sloppy.  But it's better than doing
# it by hand every time and it's a step in the right direction.  If
# anything, it's a lousy set instruction manual for building and
# testing Yokozuna.
#
# Currently requires that you add a 'yz_verify' section to your
# ~/.riak_test.config.  In the future I hope to add an option to
# riak_test to specify a config file.  It's important that the
# 'yz_verify' config agrees with the path passed as argument to this
# script.
#
# Usage:
#
#   mkdir /tmp/yz-verify
#   ./verify.sh /tmp/yz-verify | tee verify.out

function error()
{
    echo "ERROR: $1"
    exit 1
}

function info()
{
    echo "INFO: $1"
}

function maybe_clone()
{
    dir=$1; shift
    url=$1; shift

    if [ ! -e $dir ]; then
        info "cloning $url to $dir"
        if ! git clone $url $dir; then
            error "failed to clone $url to $dir"
        fi
    else
        info "$url already cloned to dir $dir"
    fi
}

function sanity_check()
{
    # TODO: It says 15B02 or greater should be installed but doesn't
    # actually verify the version.
    if ! which erl; then
        error "Erlang 15B02 or greater must be installed"
    fi
}

function verify_yokzouna()
{
    dir=$1; shift
    pushd $dir

    info "build yokozuna"
    if ! make; then
        error "failed to compile yokozuna"
    fi

    info "run yokozuna unit tests"
    if ! make test; then
        error "yokozuna unit tests failed ($tmp)"
    fi

    # if [ ! -e ~/.yokozuna_combo_dialyzer_plt ]; then
    #     info "build PLT for yokozuna"
    #     if ! make build_plt; then
    #         error "failed to build yokozuna PLT ($tmp)"
    #     fi
    # else
    #     info "yokozuna PLT already built"
    # fi

    # info "dialyze yokozuna"
    # if ! make dialyzer; then
    #     error "failed to dialyze ($tmp)"
    # fi

    # Need to build this for riak_test
    pushd misc/bench

    info "compile yokozuna bench driver"
    if ! ../../rebar get-deps; then
        error "failed to get deps for yokozuna/misc/bench"
    fi

    if ! ../../rebar compile; then
        error "failed to compile yokozuna/misc/bench"
    fi

    info "build basho_bench"
    pushd deps/basho_bench
    if ! make; then
        error "failed to build yokozuna/misc/bench/deps/basho_bench"
    fi

    popd # deps/basho_bench
    popd # misc/bench
    popd # $yz_dir
}

if [ $# != 1 ]; then
    error "incorrect number of arguments"
fi

dir=$1; shift
tmp=$dir
mkdir $dir

if [ ! -e $dir ]; then
    error "failed to mkdir $dir"
fi

pushd $tmp

yz_dir=$PWD/yokozuna

maybe_clone riak_test git://github.com/basho/riak_test.git
maybe_clone yokozuna git://github.com/basho/yokozuna.git
maybe_clone riak_yz git://github.com/basho/riak.git

pushd riak_yz
git checkout yz-merge-1.3.0
popd

pushd riak_test
info "building riak_test"
if ! make; then
    error "failed to make riak_test ($tmp)"
fi
popd

verify_yokzouna $yz_dir

pushd riak_yz

if [ ! -e dev/dev1 ]; then
    info "build riak"
    if ! make; then
        error "failed to make riak ($tmp)"
    fi

    info "build stagedevrel"
    if ! make stagedevrel; then
        error "failed to make stagedevrel ($tmp)"
    fi
fi

popd # riak_yz

export RT_DEST_DIR=$PWD/rt
if [ ! -e $RT_DEST_DIR/dev1 ]; then
    info "setup rtdev release ($RT_DEST_DIR)"
    ./riak_test/bin/rtdev-setup-releases.sh
fi

pushd $yz_dir

info "compile yokozuna riak_test tests"
make compile-riak-test

info "run yokozuna riak_test tests"
export YZ_BENCH_DIR=$yz_dir/misc/bench
if ! $tmp/riak_test/riak_test -c yz_verify -d riak_test/ebin; then
    error "yokozuna riak_test tests failed"
fi

popd

info "IF YOU ARE READING THIS THEN I THINK THINGS WORKED"
