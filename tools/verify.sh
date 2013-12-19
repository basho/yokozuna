#!/usr/bin/env bash
#
# Verify that Yokozuna...
#
# 1. compiles
# 3. passes unit tests
# 4. passes riak_test tests
#
# Currently requires that you add a 'yz_verify' section to your
# ~/.riak_test.config.  In the future I hope to add an option to
# riak_test to specify a config file.  It's important that the
# 'yz_verify' config agrees with the path passed as argument to this
# script.
#
#> Usage:
#>
#>  ./verify.sh [OPTIONS] <previous riak dir> <scratch dir>
#>
#> Options:
#>
#>  --yz-source: Where to clone Yokozuna from.  May be a local or
#>               remote git repo.
#>
#>  --yz-branch: Use a specific branch of Yokozuna.
#>
#>  --riak-source: Where to clone Riak from.
#>
#>  --riak-branch: Usage a specific branch of Riak.
#>
#>  --test: Run only a specific test.
#>
#> Example:
#>
#>  ./verify.sh ~/riak-builds/1.4.1 /tmp/yz-verify | tee verify.out
#>
#>  ./verify.sh --yz-source ~/home/user/yokozuna ~/riak-builds/1.4.1 /tmp/yz-verify | tee verify.out

# **********************************************************************
# HELPERS
# **********************************************************************

function usage()
{
    grep "#>" $0 | sed -e 's/#>//' -e '$d'
}

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
    branch=${1:-master}

    if [ ! -e $dir ]; then
        if echo "$url" | egrep "^git:|@"; then
            info "cloning branch $branch of $url to $dir"
            if ! git clone -b $branch $url $dir; then
                error "failed to clone $url to $dir"
            fi
        else
            info "linking directory $url to $dir"
            ln -s $url $dir
        fi
    else
        info "$url already cloned to dir $dir"
    fi
}

function ant_version()
{
    ant -version 2>&1 | sed -En 's/.*version ([0-9.]+).*/\1/p'
}

function erl_version()
{
    erl -eval "io:format(\"~s~n\", [erlang:system_info(system_version)]), init:stop()." | sed -n -E 's/Erlang (R[A-Z0-9a-z\-]+) .*/\1/p'
}

function sanity_check()
{
    if ! erl_version | egrep '15B02|15B03|16' > /dev/null; then
        error "Erlang 15B02 or greater required"
    fi

    if ! ant_version | egrep '1\.8\.[234]?|1\.9' > /dev/null; then
        error "Apache Ant 1.8.2 or greater required"
    fi

    if ! which wget > /dev/null; then
        error "wget is required"
    fi

    if ! which make > /dev/null; then
        error "make is required"
    fi

    if ! grep 'yz_verify' ~/.riak_test.config > /dev/null; then
        error "you must add a 'yz_verify' section to ~/.riak_test.config"
        # TODO: print exact entry that needs to be added
    fi
}

function verify_yokozuna()
{
    maybe_clone yokozuna $YZ_SRC $YZ_BRANCH
    pushd yokozuna

    info "build yokozuna"
    if ! make; then
        error "failed to compile yokozuna"
    fi

    info "run yokozuna unit tests"
    if ! make test; then
        error "yokozuna unit tests failed ($tmp)"
    fi

    popd
}

function compile_yz_bench()
{
    pushd yokozuna/misc/bench

    info "compile yokozuna bench driver"
    if ! ../../rebar get-deps; then
        error "failed to get deps for yokozuna/misc/bench"
    fi

    if ! ../../rebar compile; then
        error "failed to compile yokozuna/misc/bench"
    fi

    popd
}

function build_riak_yokozuna()
{
    maybe_clone riak_yz $RIAK_SRC $RIAK_BRANCH
    pushd riak_yz

    info "build riak_yz"

    mkdir deps
    pushd deps
    maybe_clone yokozuna $YZ_SRC $YZ_BRANCH
    popd

    if ! make; then
        error "failed to make riak_yz"
    fi

    if [ ! -e dev/dev1 ]; then
        info "build stagedevrel"
        if ! make stagedevrel; then
            error "failed to make stagedevrel"
        fi
    fi

    popd
}

function build_riak_test()
{
    maybe_clone riak_test git://github.com/basho/riak_test.git
    pushd riak_test

    info "building riak_test"
    if ! make; then
        error "failed to make riak_test"
    fi

    popd
}

function setup_rt_dir()
{
    export RT_DEST_DIR=$PWD/rt
    if [ ! -e $RT_DEST_DIR/dev1 ]; then
        info "setup rtdev release ($RT_DEST_DIR)"
        if ! ./riak_test/bin/rtdev-setup-releases.sh; then
            error "failed to run rtdev-setup-releases.sh"
        fi
    fi
}

function run_riak_test_tests()
{
    pushd yokozuna

    info "compile yokozuna riak_test tests"
    if ! make compile-riak-test; then
        error "failed to make Yokozuna's riak_test tests"
    fi

    if echo "$TEST_ONLY" > /dev/null; then
        info "only running $TEST_ONLY"
        (cd riak_test/ebin && ls | egrep -v "$TEST_ONLY|yz_rt" | xargs rm)
    fi

    info "run yokozuna riak_test tests"
    if ! $WORK_DIR/riak_test/riak_test -c yz_verify -d riak_test/ebin; then
        error "yokozuna riak_test tests failed"
    fi

    popd
}

# **********************************************************************
# SCRIPT
# **********************************************************************

sanity_check

YZ_SRC=git://github.com/basho/yokozuna.git
YZ_BRANCH=develop
RIAK_SRC=git://github.com/basho/riak.git
RIAK_BRANCH=develop
TEST_ONLY=

while [ $# -gt 0 ]
do
    case $1 in
        -h | --help)
            usage
            exit 0
            ;;
        --yz-source)
            YZ_SRC=$2
            shift
            ;;
        --yz-branch)
            YZ_BRANCH=$2
            shift
            ;;
        --riak-source)
            RIAK_SRC=$2
            shift
            ;;
        --riak-branch)
            RIAK_BRANCH=$2
            shift
            ;;
        --test)
            TEST_ONLY=$2
            shift
            ;;
        -*)
            error "unrecognized option: $1"
            ;;
        *)
            break
            ;;
    esac
    shift
done

if [ $# != 2 ]; then
    echo "ERROR: incorrect number of arguments"
    usage
    exit 1
fi

PREV_DIR=$1; shift

WORK_DIR=$1; shift
mkdir $WORK_DIR
if [ ! -e $WORK_DIR ]; then
    error "failed to create dir $WORK_DIR"
fi
cd $WORK_DIR
# This will make WORK_DIR absolute if it isn't already
WORK_DIR=$(pwd)

ln -s $PREV_DIR $(basename $PREV_DIR)

verify_yokozuna
compile_yz_bench
build_riak_yokozuna
build_riak_test
setup_rt_dir
run_riak_test_tests

info "IF YOU ARE READING THIS THEN I THINK THINGS WORKED"
