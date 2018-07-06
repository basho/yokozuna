#!/usr/bin/env sh
#
# Build JAR file containing customer Solr request handlers.
set -eu

[ $(basename $PWD) == "tools" ] || cd tools
. common.sh


if ! [ ${FORCE_REBUILD:-0} -gt 0 ]; then
    if [ -f $YZ_JAR_NAME ] && [ -f $MON_JAR_NAME ]; then
        echo "Using pre-packages yokozuna jar files"
        exit 0
    fi
fi


if [ ! -x "`which javac`" ] || [ ! -x "`which jar`" ]; then
    echo "Couldn't find javac and/or jar, which is needed to compile Yokozuna."
    exit 1
fi

if ! javac -version 2>&1 | egrep $'(?:1\.[789]|10)\.'
then
    echo "JDK 1.8+ must be used to compile these jars"
    exit 1
fi

if [ $(basename $PWD) != "tools" ]
then
    echo "This script must be run from tools directory"
    exit 1
fi


echo "Building ${YZ_JAR_NAME} ${MON_JAR_NAME}..."

SOLR_DIR=../priv/solr
SOLR_WAR=$SOLR_DIR/webapps/solr.war
SOLR_JAR_DIR=../build/solr-jars

if [ -e $SOLR_WAR ]; then
    echo "$SOLR_WAR is already there"
else
    echo "Downloading the Solr package..."
    ./grab-solr.sh
fi

# if [ ! -e $SOLR_JAR_DIR ]; then
#     echo "Exploding ${SOLR_WAR}..."
#     mkdir $SOLR_JAR_DIR
#     cp $SOLR_WAR $SOLR_JAR_DIR
#     (cd $SOLR_JAR_DIR                           \
#      && jar xf solr.war WEB-INF/lib             \
#      && mv WEB-INF/lib/* .                      \
#      && rm -rf WEB-INF solr.war)
#     # copy logging jars
#     cp $SOLR_DIR/lib/ext/* $SOLR_JAR_DIR
# fi


echo "Compiling..."
printf "SOLR_JAR_DIR = %s\n" "${SOLR_JAR_DIR}"

# javac -cp "$SOLR_JAR_DIR/*"                     \
#     ../java_src/com/basho/yokozuna/*/*.java     \
#     ../java_src/com/basho/yokozuna/*/*/*.java

echo "Creating ${YZ_JAR_NAME}..."
mkdir -p ../priv/java_lib

# jar -cvf $YZ_JAR_NAME \
#   -C ../java_src/ com/basho/yokozuna/handler \
#   -C ../java_src/ com/basho/yokozuna/query
(cd ../java_src \
     && ant all \
            -Dyz_vsn=${YZ_JAR_VSN} -Dyz_mon_vsn=${MON_JAR_VSN} \
            -Dsolr_vsn=${SOLR_VSN}
    )

cp ../yz-build/$YZ_JAR_NAME .
mk_sha $YZ_JAR_NAME $YZ_JAR_SHA

echo "Finished building yokozuna.jar..."

# monitor has to be packaged separately because it relies on the
# dynamic classpath the jetty/solr set up
echo "--- ${YZ_JAR_NAME}" && ls -l ${YZ_JAR_NAME}*

# jar cvf  $MON_JAR_NAME \
#   -C ../java_src/ com/basho/yokozuna/monitor

cp ../yz-build/$MON_JAR_NAME .
mk_sha $MON_JAR_NAME $MON_JAR_SHA

echo "--- ${MON_JAR_NAME}" && ls -l ${MON_JAR_NAME}*

