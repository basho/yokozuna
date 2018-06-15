#!/usr/bin/env sh
#
# Build JAR file containing customer Solr request handlers.
set -e


function sha
{
    file=$1
    sha_file=$2

    if which sha1sum; then
        sha1sum $file > $sha_file
    elif which shasum; then
        shasum -a 1 $file > $sha_file
    else
        echo "Unable to locate program to compute SHA1"
        exit 1
    fi
}

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


echo "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% NOTICE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
echo "%                                                                          %"
echo "% If building new jars to be uploaded to s3 then make sure to update the   %"
echo "% YZ_JAR_VSN and MON_JAR_VSN variables.                                    %"
echo "%                                                                          %"
echo "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"

echo "Build the yokozuna.jar..."

SOLR_DIR=../priv/solr
SOLR_WAR=$SOLR_DIR/webapps/solr.war
SOLR_JAR_DIR=../build/solr-jars

if [ ! -e $SOLR_WAR ]; then
    echo "Downloading the Solr package..."
    ./grab-solr.sh
fi

if [ ! -e $SOLR_JAR_DIR ]; then
    echo "Exploding ${SOLR_WAR}..."
    mkdir $SOLR_JAR_DIR
    cp $SOLR_WAR $SOLR_JAR_DIR
    (cd $SOLR_JAR_DIR                           \
     && jar xf solr.war WEB-INF/lib             \
     && mv WEB-INF/lib/* .                      \
     && rm -rf WEB-INF solr.war)
    # copy logging jars
    cp $SOLR_DIR/lib/ext/* $SOLR_JAR_DIR
fi


echo "Compiling..."
printf "SOLR_JAR_DIR = %s\n" "${SOLR_JAR_DIR}"

# javac -cp "$SOLR_JAR_DIR/*"                     \
#     ../java_src/com/basho/yokozuna/*/*.java     \
#     ../java_src/com/basho/yokozuna/*/*/*.java

YZ_JAR_VSN=3
YZ_JAR_NAME=yokozuna-$YZ_JAR_VSN.jar
YZ_JAR_SHA=$YZ_JAR_NAME.sha

echo "Creating ${YZ_JAR_NAME}..."
mkdir -p ../priv/java_lib

# jar -cvf $YZ_JAR_NAME \
#   -C ../java_src/ com/basho/yokozuna/handler \
#   -C ../java_src/ com/basho/yokozuna/query
(cd ../java_src && ant -f yokozuna.xml all)
cp ../yz-build/yz-handler.jar $YZ_JAR_NAME
sha $YZ_JAR_NAME $YZ_JAR_SHA

echo "Finished building yokozuna.jar..."

# monitor has to be packaged separately because it relies on the
# dynamic classpath the jetty/solr set up
echo "Create ${YZ_JAR_NAME}..."

MON_JAR_VSN=1
MON_JAR_NAME=yz_monitor-$MON_JAR_VSN.jar
MON_JAR_SHA=$MON_JAR_NAME.sha
# jar cvf  $MON_JAR_NAME \
#   -C ../java_src/ com/basho/yokozuna/monitor

cp ../yz-build/yz-monitor.jar $MON_JAR_NAME

sha $MON_JAR_NAME $MON_JAR_SHA

echo "Finished building yz_monitor.jar..."
