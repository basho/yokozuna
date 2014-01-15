#!/usr/bin/env bash
#
# Build a source package of Yokozuna.
#
## SYNOPSIS
##
##    ./src-pkg.sh [-rt riak_tag] working_dir version
##
## EXAMPLE
##
##    ./src-pkg.sh /tmp 0.3.0
##
##    ./src-pkg.sh -rt riak-2.0.0pre9 /tmp 0.13.0
set -e

usage() {
    echo
    grep '##' $0 | sed -r 's/##//' | sed '$d'
}

error() {
    echo ERROR: $1
    usage
    exit 1
}

while [ $# -gt 0 ]
do
    case $1 in
        -rt)
            riak_tag=$2
            shift
            ;;
        -h)
            usage
            exit 0
            ;;
        -*)
            error "unrecognized option $1"
            ;;
        *)
            break
            ;;
    esac
    shift
done

if [ ! $# -eq 2 ]; then
    error "incorrect number of arguments"
fi

WD=$1; shift
VSN=$1
RIAK_DIR=riak-yokozuna-$VSN-src
TGZ=$RIAK_DIR.tar.gz

pushd $WD                       # in working dir

if [ ! -d $RIAK_DIR ]; then
    git clone git://github.com/basho/riak.git $RIAK_DIR
fi

pushd $RIAK_DIR                 # in riak

if [ -n $riak_tag ]
then
    git checkout $riak_tag
fi

if [ -n $riak_tag ]
then
    cp rebar.config.lock rebar.config
fi

sed -i"bak" -e "/{yokozuna.*/{
N
N
s#{yokozuna.*#{yokozuna, \".*\", {git, \"git://github.com/basho/yokozuna.git\", {tag, \"v${VSN}\"}}},#
}" rebar.config

git commit -am "Checkout Yokozuna version v$VSN"

make PKG_ID="$RIAK_DIR" dist

tar zxvf $TGZ
pushd $RIAK_DIR/deps/yokozuna   # in extracted pkg
./tools/grab-solr.sh
rm -rf build/solr-4*
popd                            # out extracted pkg
tar -zcvf $TGZ $RIAK_DIR

popd                            # out riak
popd                            # out working dir
