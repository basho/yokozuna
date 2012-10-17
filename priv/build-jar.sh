#!/bin/bash
#
# Temporary script to build JAR file containing customer Solr request
# handlers.

if [ $(basename $PWD) != "priv" ]
then
    cd priv
fi

SOLR_JAR_DIR=solr-jars/WEB-INF/lib

if [ ! -e $SOLR_JAR_DIR ]; then
    mkdir solr-jars
    cp solr/webapps/solr.war solr-jars
    pushd solr-jars
    jar xvf solr.war
    popd
fi

javac -cp "$SOLR_JAR_DIR/*" java/com/basho/yokozuna/handler/*.java java/com/basho/yokozuna/query/*.java
jar cvf yokozuna.jar -C java/ .

mkdir java_lib
cp yokozuna.jar java_lib
