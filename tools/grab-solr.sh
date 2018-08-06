#!/usr/bin/env sh
#
# Script to grab Solr and embed in priv dir. This script assumes it is
# being called from root dir or tools dir.
#
# Usage:
#     ./grab-solr.sh
#
#     specify SOLR_PKG_DIR to skip the solr download and use a local copy

set    -eu

[ $(basename $PWD) == "tools" ] || cd tools
. ./common.sh


PRIV_DIR=../priv
CONF_DIR=$PRIV_DIR/conf

SOLR_DIR=$PRIV_DIR/solr
FRESH_SOLR=1

BUILD_DIR=../build
VSN=solr-7.3.1

FILENAME=$VSN.tgz
TMP_DIR="$(pwd)/tmp"
TMP_FILE=$TMP_DIR/$FILENAME
SRC_DIR=$BUILD_DIR/$VSN

EXAMPLE_DIR=$SRC_DIR/example
SERVER_DIR=$SRC_DIR/server

# http://apache.cs.uu.nl/lucene/solr/7.3.1/solr-7.3.1.tgz
# https://www.apache.org/dist/lucene/solr/7.3.1/solr-7.3.1.tgz
# https://www.apache.org/dist/lucene/solr/7.3.1/solr-7.3.1.tgz.asc
# https://www.apache.org/dist/lucene/solr/7.3.1/solr-7.3.1.tgz.sha1
ARTIFACT_URL_PREFIX=https://archive.apache.org/dist/lucene/solr/7.3.1/

check_for_solr()
{
    # $SOLR_DIR is preloaded with xml files, so check for the generated jar
    [ -e $SOLR_DIR/start.jar ]
}

get_solr()
{
    if [ -n "${SOLR_PKG_DIR:-}" ] && [ -d ${SOLR_PKG_DIR} ]
    then
        # This is now obsolete thanks to implicit caching above
        # but will leave in for now as to not break anyone.
        echo "Using local copy of Solr $SOLR_PKG_DIR/$FILENAME"
        cp $SOLR_PKG_DIR/$FILENAME .
    else
        if [ -e $TMP_FILE ]; then
            echo "Using cached copy of Solr at $TMP_FILE"
            ln -s $TMP_FILE $FILENAME
        else
            echo "Downloading original Solr..."
            download "${ARTIFACT_URL_PREFIX}/$FILENAME"
            download "${ARTIFACT_URL_PREFIX}/$FILENAME.sha1"
            set -u; shasum -s -c $FILENAME.sha1
            mkdir -p -m 1777 $TMP_DIR
            cp $FILENAME $TMP_DIR
        fi
    fi

    tar -xf $FILENAME
    echo "OK, tar = ${FILENAME}"
}

if check_for_solr && [ ${FORCE_REBUILD:-0} -gt 0 ]; then
    echo "Solr is there, $SOLR_DIR"
else
    mkdir -p $BUILD_DIR && cd $BUILD_DIR

    if [ ! -e $SRC_DIR ]
    then
        get_solr
    fi

    # Explicitly copy files needed rather than copying everything and
    # removing which requires using cp -rn (since $SOLR_DIR/etc has files
    # which shouldn't be overwritten).  For whatever reason, cp -n causes
    # non-zero exit code when files that would have been overwritten are
    # detected.
    # cp -r $EXAMPLE_DIR/etc/create-solrtest.keystore.sh $SOLR_DIR/etc
    cp -r $SERVER_DIR/etc/webdefault.xml $SOLR_DIR/etc
    cp -r $SERVER_DIR/lib $SOLR_DIR
    cp -r $SERVER_DIR/solr/configsets/_default/conf/lang $CONF_DIR
    cp    $SERVER_DIR/solr/configsets/_default/conf/protwords.txt $CONF_DIR
    cp    $SERVER_DIR/solr/configsets/_default/conf/stopwords.txt $CONF_DIR
    cp    $SERVER_DIR/solr/configsets/_default/conf/synonyms.txt $CONF_DIR
    # cp $COL1_DIR/conf/mapping-* $CONF_DIR
    # TODO: does resources need to be copied?
    cp -r $SERVER_DIR/resources $SOLR_DIR
    cp -r $SERVER_DIR/modules   $SOLR_DIR
    cp -r $SERVER_DIR/solr-webapp $SOLR_DIR
    cp    $SERVER_DIR/start.jar $SOLR_DIR
    # cp -r $SERVER_DIR/webapps $SOLR_DIR

    echo "Solr dir created successfully"
fi

