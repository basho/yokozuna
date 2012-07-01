#!/bin/bash
#
# Temporary script to build JAR file containing customer Solr request
# handlers.

if [ $(basename $PWD) != "priv" ]
then
    cd priv
fi

SOLR_JAR_DIR=${SOLR_JAR_DIR:-'/usr/local/apache-solr-3.5.0/example/work/Jetty_0_0_0_0_8983_solr.war__solr__k1kf17/webapp/WEB-INF/lib/*'}

javac -cp "$SOLR_JAR_DIR" java/com/basho/yokozuna/handler/MerkleTreeHandler.java
jar cvf yokozuna.jar -C java/ .

mkdir java_lib
cp yokozuna.jar java_lib