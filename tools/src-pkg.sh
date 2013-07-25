#!/usr/bin/env bash
#
# Build a source package of Yokozuna.
#
#> Usage:
#>
#>    ./src-pkg.sh <working dir> <version>
#>
#> Example:
#>
#>    ./src-pkg.sh /tmp 0.3.0

usage() {
    echo
    grep '#>' $0 | tr -d '#>' | sed '$d'
}

error() {
    echo ERROR: $1
    usage
    exit 1
}

if [ ! $# -eq 2 ]; then
    error "incorrect number of arguments"
fi

WD=$1; shift
VSN=$1
RIAK_DIR=riak-yokozuna-$VSN-src
TGZ=$RIAK_DIR.tar.gz

pushd $WD
git clone -b rz-yz-merge-1.4.0 git://github.com/basho/riak.git $RIAK_DIR
pushd $RIAK_DIR
make deps
pushd deps/yokozuna
git checkout v$VSN
./priv/grab-solr.sh
rm -rf priv/solr-4*
popd
popd

rm -f $TGZ
tar -zcvf $TGZ $RIAK_DIR
popd
