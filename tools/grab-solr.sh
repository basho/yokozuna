#!/usr/bin/env bash
#
# Script to grab Solr and embed in priv dir. This script assumes it is
# being called from root dir or tools dir.
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

PRIV_DIR=../priv
CONF_DIR=$PRIV_DIR/conf
SOLR_DIR=$PRIV_DIR/solr
BUILD_DIR=../build
VSN=solr-4.7.0-yz-1
FILENAME=$VSN.tgz
TMP_DIR=/var/tmp/yokozuna
TMP_FILE=$TMP_DIR/$FILENAME
SRC_DIR=$BUILD_DIR/$VSN
EXAMPLE_DIR=$SRC_DIR/example
COL1_DIR=$EXAMPLE_DIR/solr/collection1

check_for_solr()
{
    # $SOLR_DIR is preloaded with xml files, so check for the generated jar
    test -e $SOLR_DIR/start.jar
}

download()
{
    if which wget > /dev/null; then
        wget --no-check-certificate --progress=dot:mega $1
    elif which curl > /dev/null; then
        curl --insecure --progress-bar -O $1
    fi
}

get_solr()
{
        if [[ -z ${SOLR_PKG_DIR+x} ]]
        then
            if [ -e $TMP_FILE ]; then
                echo "Using cached copy of Solr $TMP_FILE"
                ln -s $TMP_FILE $FILENAME
            else
                echo "Pulling Solr from S3"
                download "http://s3.amazonaws.com/files.basho.com/solr/$FILENAME"
                if [ -d $TMP_DIR ]; then
                    cp $FILENAME $TMP_DIR
                else
                    mkdir $TMP_DIR
                    cp $FILENAME $TMP_DIR
                fi
            fi
        else
            # This is now obsolete thanks to implicit caching above
            # but will leave in for now as to not break anyone.
            echo "Using local copy of Solr $SOLR_PKG_DIR/$FILENAME"
            cp $SOLR_PKG_DIR/$FILENAME ./
        fi
        tar zxf $FILENAME
}

if ! check_for_solr
then

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
    cp -r $EXAMPLE_DIR/etc/create-solrtest.keystore.sh $SOLR_DIR/etc
    cp -r $EXAMPLE_DIR/etc/webdefault.xml $SOLR_DIR/etc
    cp -r $EXAMPLE_DIR/lib $SOLR_DIR
    cp -r $COL1_DIR/conf/lang $CONF_DIR
    cp $COL1_DIR/conf/protwords.txt $CONF_DIR
    cp $COL1_DIR/conf/stopwords.txt $CONF_DIR
    cp $COL1_DIR/conf/synonyms.txt $CONF_DIR
    cp $COL1_DIR/conf/mapping-* $CONF_DIR
    # TODO: does resources need to be copied?
    cp -r $EXAMPLE_DIR/resources $SOLR_DIR
    cp -r $EXAMPLE_DIR/solr-webapp $SOLR_DIR
    cp -r $EXAMPLE_DIR/start.jar $SOLR_DIR
    cp -r $EXAMPLE_DIR/webapps $SOLR_DIR

    echo "Solr dir created successfully"
fi

JAVA_LIB=../priv/java_lib
YZ_JAR_VSN=1
YZ_JAR_NAME=yokozuna-$YZ_JAR_VSN.jar

if [ ! -e $JAVA_LIB/$YZ_JAR_NAME ]
then
    if [ ! -d $JAVA_LIB ]
    then
        mkdir $JAVA_LIB
    fi

    echo "Downloading $YZ_JAR_NAME"
    download "http://s3.amazonaws.com/files.basho.com/yokozuna/$YZ_JAR_NAME"
    mv $YZ_JAR_NAME $JAVA_LIB/$YZ_JAR_NAME
fi

EXT_LIB=../priv/solr/lib/ext
MON_JAR_VSN=1
MON_JAR_NAME=yz_monitor-$MON_JAR_VSN.jar

if [ ! -e $EXT_LIB/$MON_JAR_NAME ]
then
    echo "Downloading $MON_JAR_NAME"
    download "http://s3.amazonaws.com/files.basho.com/yokozuna/$MON_JAR_NAME"
    mv $MON_JAR_NAME $EXT_LIB/$MON_JAR_NAME
fi
