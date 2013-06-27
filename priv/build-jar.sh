#!/usr/bin/env bash
#
# Temporary script to build JAR file containing customer Solr request
# handlers.

if [ $(basename $PWD) != "priv" ]
then
    cd priv
fi

echo "Build the yokozuna.jar..."

SOLR_JAR_DIR=solr-jars

if [ ! -e $SOLR_JAR_DIR ]; then
    echo "Explode the WAR..."
    mkdir solr-jars
    cp solr/webapps/solr.war solr-jars
    pushd solr-jars
    jar xf solr.war WEB-INF/lib
    mv WEB-INF/lib/* .
    rm -rf WEB-INF solr.war
    popd
    # copy logging jars
    cp solr/lib/ext/* solr-jars
fi

echo "Compile..."
javac -cp "$SOLR_JAR_DIR/*" java/com/basho/yokozuna/handler/*.java java/com/basho/yokozuna/query/*.java java_monitor/com/basho/yokozuna/monitor/*.java
echo "Create yokozuna.jar..."
mkdir java_lib
jar cvf java_lib/yokozuna.jar -C java/ .
echo "Finished building yokozuna.jar..."

echo "Create yz_monitor.jar..."
jar cvf solr/lib/ext/yz_monitor.jar -C java_monitor/ .
echo "Finished building yz_monitor.jar..."
