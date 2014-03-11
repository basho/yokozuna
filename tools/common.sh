#! /bin/sh
#
# This script contains common functions used by other scripts.
#
# TODO: divide into API/helper section
#
# TODO: alphabetize


# This function produces a date-time stamp.
function dt()
{
    date "+%Y-%m-%d %H:%M:%S"
}

# This function is used to submit INFO messages to stdout.
function info()
{
    dt=$(dt)
    echo "$dt [INFO] $1"
}

# This function is used to submit ERROR messages to stderr. An error
# indicates something went irreversibly wrong and the program should
# exit with a non-success.
function error()
{
    echo "$dt [ERROR] $1" 1>&2
    exit 1
}


# TODO: redo doc
#
# This function clones the passed $url if an only if
#
# 1. the directory $dir doesn't exist already, and
# 2. the passed $url is not a directory.
#
# If the $url is a directory then a symlink will be established.
#
# maybe_clone dir url [branch]
function maybe_clone()
{
    dir=$1; shift
    src=$1; shift

    if [ ! -e $dir ]; then
        if is_git $src; then
            url=$(get_url $src)
            branch=$(get_branch $src)
            info "cloning branch $branch of $url to $dir"
            if ! git clone -b $branch $url $dir; then
                error "failed to clone $url to $dir"
            fi
        else
            info "linking directory $url to $dir"
            ln -s $src $dir
        fi
    else
        info "dir $dir already exists; taking no action"
    fi
}

# SYNOPSIS
#
#   get_url "<url> <branch>"
#
# DESCRIPTION
#
#   Given a string of "$url $branch" this function will echo the $url
#   to stdout.
function get_url()
{
    echo ${1%% *}
}

# SYNOPSIS
#
#   get_branch "<url> <branch>"
#
#   get_branch "<url>"
#
# DESCRIPTION
#
#   Given a string of "$url $branch" this function will echo the
#   $branch to stdout. Given the string "$url" it will echo "develop".
function get_branch()
{
    if echo $1 | egrep ' ' > /dev/null; then
        echo ${1##* }
    else
        echo "develop"
    fi
}

function is_git()
{
    if echo $1 | egrep "^git:|^git@|^https:" > /dev/null; then
        return 0;
    else
        return 1;
    fi
}

# SYNOPSIS
#
# build_riak <dir> <riak_src> [-l|--dep-mod-local <dep_name>=<local_dep_path>] [-r|--dep-mod-rev <dep_name>=<branch_name>]
#
# DESCRIPTION
#
# Build a version of Riak in <dir> based on <riak_src> with an
# optional list of depedency modifications.
#
# <dir> - The name of the root directory for the Riak tree.
#
# <riak_src> - The source from which to build Riak. This may be a git
# url with optional branch/tag/sha, or a link to a source tar.gz.
#
# <dep_versions> - A pipe ('|') separated list of "<dep_name>
#
# EXAMPLES
#
# Build the latest Riak development version:
#
#     build_riak riak_yz_dev https://github.com/basho/riak.git
#
# Build the latest Riak development version with local working copy of
# Yokozuna:
#
#     build_riak riak_yz_dev https://github.com/basho/riak.git -l yokozuna=~/work/yokozuna
#
# Build the latest Riak development version with local working copy of
# Yokozuna and branch 'foo' of riak_kv:
#
#     build_riak riak_yz_dev https://github.com/basho/riak.git -l yokozuna=~/work/yokozuna -r riak_kv=foo
function build_riak()
{
    dir=$1; shift
    riak_src=$1; shift

    if [ ! -e $dir/dev/dev1 ]; then
        maybe_clone $dir $riak_src
        pushd $dir

        info "get deps for $dir"
        if ! make deps; then
            error "failed to get deps"
        fi

        pushd deps                  # in deps

        while [ $# -gt 0 ]; do
            case $1 in
                --dep-mod-local | -l)
                    if [ -z $2 ]; then
                        error "must pass <dep_name>=<path> to -l option"
                    fi
                    name=${2%%=*}
                    path=${2##*=}
                    info "linking $name to $path"
                    rm -rf $name
                    ln -s $path $name
                    shift
                    ;;
                --dep-mod-rev | -r)
                    if [ -z $2 ]; then
                        error "must pass <dep_name>=<rev> to -r option"
                    fi
                    name=${2%%=*}
                    rev=${2##*=}
                    info "checking out revision $rev in $name"
                    pushd $name
                    if ! git checkout $rev; then
                        error "failed to checkout revision '$rev'"
                    fi
                    popd
                    shift
                    ;;
                -*)
                    error "unknown option '$1' to build_riak"
                    ;;
                *)
                    error "unknown argument '$1' to build_riak"
            esac
            shift
        done

        popd                        # out deps

        if ! make; then
            error "failed to make $dir"
        fi

        info "build stagedevrel"

        if ! make stagedevrel; then
            error "failed to make stagedevrel"
        fi

        popd                    # out $dir
    else
        info "a stagedevrel build of $dir already exists, doing nothing"
    fi
}

# N.B. Be careful where you run this function. It will pick up all
# */dev directories in the CWD.
function setup_rt_dir()
{
    rt_dir=$1; shift
    dest=$1; shift

    # The RT_DEST_DIR variable is used by the rtdev-setup-releases
    # script.
    export RT_DEST_DIR=$dest/rt
    if [ ! -e $RT_DEST_DIR/*/dev ]; then
        info "setup rtdev release at $RT_DEST_DIR"
        if ! $rt_dir/bin/rtdev-setup-releases.sh; then
            error "failed to run rtdev-setup-releases.sh"
        fi
    else
        info "rtdev already setup at $RT_DEST_DIR"
    fi
}

function generate_bench_rt_cfg()
{
    yz_dir=$1; shift
    rt_bench_root=$1; shift
    previous=$rt_bench_root/$1; shift
    current=$rt_bench_root/$1; shift

    # 1 hour
    rt_max_wait_time=$((60 * 60 * 1000))

    cat <<EOF

{default,
 [
  {rt_max_wait_time, $rt_max_wait_time},
  {rt_retry_delay, 1000},
  {rt_harness, rtdev},
  {rt_scratch_dir, "/tmp/riak_test_scratch"},
  {basho_bench, "/root/work/basho_bench"},
  {lager_level, info},
  {rt_cookie, riak}
 ]
}.

{yz_bench,
 [
  {rt_project, "riak"},
  {yz_dir, "$yz_dir"},

  {rtdev_path,
   [
    {root, "$rt_bench_root"},
    {previous, "$previous"},
    {current, "$current"}
   ]}
]}.

EOF
}
