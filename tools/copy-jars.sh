#!/usr/bin/env sh
set -eu

[ $(basename $PWD) = "tools" ] || cd tools
. ./common.sh

JAVA_LIB=../priv/java_lib

YZ_ARTIFACTS_URL="."

if [ ! -f $JAVA_LIB/$YZ_JAR_NAME ] || [ ${FORCE_REBUILD:-0} -gt 0 ]
then
    mkdir -p $JAVA_LIB
    echo "Downloading $YZ_JAR_NAME"
    cp -v $YZ_JAR_NAME $JAVA_LIB
fi

EXT_LIB=../priv/solr/lib/ext

if [ ! -f $EXT_LIB/$MON_JAR_NAME ] || [ ${FORCE_REBUILD:-0} -gt 0 ]
then
    echo "Downloading $MON_JAR_NAME"
    cp -v $MON_JAR_NAME $EXT_LIB
fi

echo Done.


