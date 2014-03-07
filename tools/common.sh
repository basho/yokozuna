#! /bin/sh
#
# This script contains common functions used by other scripts.


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

# This function builds a copy of riak with the specified 
function build_riak_yokozuna()
{
    dir=$1; shift
    riak_src=$1; shift
    yz_src=$1; shift

    if [ ! -e $dir/dev/dev1 ]; then
        maybe_clone $dir $riak_src
        pushd $dir

        info "get deps for $dir"
        if ! make deps; then
            error "failed to get deps"
        fi

        mkdir deps
        pushd deps                  # in deps

        if is_git $yz_src; then
            yz_branch=$(get_branch $yz_src)
            info "using branch $yz_branch of Yokozuna"
            cd yokozuna
            git checkout $yz_branch
        else
            info "using local working copy of Yokozuna at $dir"
            rm -rf yokozuna
            maybe_clone yokozuna $yz_src
        fi

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
