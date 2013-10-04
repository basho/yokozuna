#!/usr/bin/env bash
#
# Script to grab Solr and embed in priv dir.
#
# Usage:
#     ./grab-solr.sh
#
#     specify SOLR_PKG_DIR to skip the solr download and use a local copy
set -e

if [ $(basename $PWD) != "tools" ]
then
    cd tools
fi

SOLR_DIR=../priv/solr
BUILD_DIR=../build
VSN=solr-4.3.0-yz
FILENAME=$VSN.tgz
TMP_DIR=/tmp/yokozuna
TMP_FILE=$TMP_DIR/$FILENAME
SRC_DIR=$BUILD_DIR/$VSN
EXAMPLE_DIR=$SRC_DIR/example

check_for_solr()
{
    # $SOLR_DIR is preloaded with xml files, so check for the generated jar
    test -e $SOLR_DIR/start.jar
}

get_solr()
{
        if [[ -z ${SOLR_PKG_DIR+x} ]]
        then
            if [ -e /tmp/yokozuna/$VSN.tgz ]; then
                echo "Using cached copy of Solr $TMP_FILE"
                ln -s $TMP_FILE $FILENAME
            else
                echo "Pulling Solr from S3"
                wget --no-check-certificate --progress=dot:mega https://s3.amazonaws.com/yzami/pkgs/$FILENAME
                mkdir $TMP_DIR
                cp $FILENAME $TMP_DIR
            fi
        else
            # This is now obsolete thanks to implicit caching above
            # but will leave in for now as to not break anyone.
            echo "Using local copy of Solr $SOLR_PKG_DIR/$FILENAME"
            cp $SOLR_PKG_DIR/$FILENAME ./
        fi
        tar zxf $FILENAME
}

if check_for_solr
then
    echo "Solr already exists, exiting"
    exit 0
fi

echo "Create dir $BUILD_DIR"
if [ ! -e $BUILD_DIR ]; then
    mkdir $BUILD_DIR
fi

cd $BUILD_DIR

if [ ! -e $SRC_DIR ]
then
    get_solr
fi

echo "Creating Solr dir $SOLR_DIR"

# Explicitly copy files needed rather than copying everything and
# removing which requires using cp -rn (since $SOLR_DIR/etc has files
# which shouldn't be overwritten).  For whatever reason, cp -n causes
# non-zero exit code when files that would have been overwritten are
# detected.
cp -r $EXAMPLE_DIR/contexts $SOLR_DIR
cp -r $EXAMPLE_DIR/etc/create-solrtest.keystore.sh $SOLR_DIR/etc
cp -r $EXAMPLE_DIR/etc/logging.properties $SOLR_DIR/etc
cp -r $EXAMPLE_DIR/etc/webdefault.xml $SOLR_DIR/etc
cp -r $EXAMPLE_DIR/lib $SOLR_DIR
# TODO: does resources need to be copied?
cp -r $EXAMPLE_DIR/resources $SOLR_DIR
cp -r $EXAMPLE_DIR/solr-webapp $SOLR_DIR
cp -r $EXAMPLE_DIR/start.jar $SOLR_DIR
cp -r $EXAMPLE_DIR/webapps $SOLR_DIR

echo "Solr dir created successfully"
