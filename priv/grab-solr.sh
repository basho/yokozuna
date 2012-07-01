#!/bin/bash
#
# Script to grab Solr and embed in priv dir.
#
# Usage:
#     ./grab-solr.sh

dir=solr
src_dir=apache-solr-3.5.0

check_for_solr()
{
    test -e $dir
}

if check_for_solr
then
    echo "Solr already exists, exiting..."
    exit 0
fi

if [ ! -e $src_dir ]
then
    wget http://apache.deathculture.net/lucene/solr/3.5.0/apache-solr-3.5.0.tgz
    tar zxvf apache-solr-3.5.0.tgz
fi

cp -vr $src_dir/example solr
mkdir $dir/yokozuna
cp -v solr.xml $dir/yokozuna
