#!/usr/bin/env sh
set -eu
source common.sh

JAVA_LIB=../priv/java_lib

YZ_ARTIFACTS_URL="$(realpath ../yz-build)"

if [[ ! -f $JAVA_LIB/$YZ_JAR_NAME ]]
then
    mkdir -p $JAVA_LIB
    echo "Downloading $YZ_JAR_NAME"
    # download "${ARTIFACT_URL_PREFIX}/yokozuna/$YZ_JAR_NAME"
    download ${YZ_ARTIFACTS_URL}/${YZ_JAR_NAME}
    mv $YZ_JAR_NAME $JAVA_LIB
fi

EXT_LIB=../priv/solr/lib/ext

if [[ ! -f $EXT_LIB/$MON_JAR_NAME ]]
then
    echo "Downloading $MON_JAR_NAME"
    #download "${ARTIFACT_URL_PREFIX}/yokozuna/$MON_JAR_NAME"
    download ${YZ_ARTIFACTS_URL}/${MON_JAR_NAME}
    mv $MON_JAR_NAME $EXT_LIB
fi

echo Done.


