#!/usr/bin/env bash
#
# Temporary script to build JAR file containing customer Solr request
# handlers.

if [ $(basename $PWD) != "priv" ]
then
    cd priv
fi

echo "Build the yokozuna.jar..."

SOLR_JAR_DIR=solr-jars/WEB-INF/lib

if [ ! -e $SOLR_JAR_DIR ]; then
    echo "Explode the WAR..."
    mkdir solr-jars
    cp solr/webapps/solr.war solr-jars
    pushd solr-jars
    jar xf solr.war
    popd
fi

echo "Compile..."
javac -cp "$SOLR_JAR_DIR/*" java/com/basho/yokozuna/handler/*.java java/com/basho/yokozuna/query/*.java
echo "Create yokozuna.jar..."
mkdir java_lib
jar cvf java_lib/yokozuna.jar -C java/ .
echo "Finished building yokozuna.jar..."
