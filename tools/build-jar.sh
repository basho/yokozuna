#!/usr/bin/env bash
#
# Temporary script to build JAR file containing customer Solr request
# handlers.
set -e

if [ -z "$SKIP_JAVA" ]; then
    if [ ! -x "`which javac`" ] || [ ! -x "`which jar`" ]; then
        echo "Couldn't find javac and/or jar, which is needed to compile Yokozuna."
        exit 1
    fi
fi

if [ $(basename $PWD) != "tools" ]
then
    cd tools
fi

echo "Build the yokozuna.jar..."

SOLR_DIR=../priv/solr
SOLR_JAR_DIR=../build/solr-jars

if [ ! -e $SOLR_JAR_DIR ]; then
    echo "Explode the WAR..."
    mkdir $SOLR_JAR_DIR
    cp $SOLR_DIR/webapps/solr.war $SOLR_JAR_DIR
    pushd $SOLR_JAR_DIR
    jar xf solr.war WEB-INF/lib
    mv WEB-INF/lib/* .
    rm -rf WEB-INF solr.war
    popd
    # copy logging jars
    cp $SOLR_DIR/lib/ext/* $SOLR_JAR_DIR
fi

echo "Compile..."
javac -cp "$SOLR_JAR_DIR/*" ../java_src/com/basho/yokozuna/handler/*.java ../java_src/com/basho/yokozuna/query/*.java ../java_src/com/basho/yokozuna/monitor/*.java
echo "Create yokozuna.jar..."
if [ ! -e "../priv/java_lib" ]; then
    mkdir ../priv/java_lib
fi
jar cvf ../priv/java_lib/yokozuna.jar \
  -C ../java_src/ com/basho/yokozuna/handler \
  -C ../java_src/ com/basho/yokozuna/query
echo "Finished building yokozuna.jar..."

# monitor has to be packaged separately because it relies on the
# dynamic classpath the jetty/solr set up
echo "Create yz_monitor.jar..."
jar cvf $SOLR_DIR/lib/ext/yz_monitor.jar \
  -C ../java_src/ com/basho/yokozuna/monitor
echo "Finished building yz_monitor.jar..."
