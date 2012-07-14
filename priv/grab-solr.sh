#!/bin/bash
#
# Script to grab Solr and embed in priv dir.
#
# Usage:
#     ./grab-solr.sh

dir=solr
src_dir=apache-solr-4.0.0-alpha

check_for_solr()
{
    test -e $dir
}

if [ $(basename $PWD) != "priv" ]
then
    cd priv
fi

if check_for_solr
then
    echo "Solr already exists, exiting..."
    exit 0
fi

if [ ! -e $src_dir ]
then
    wget http://apache.deathculture.net/lucene/solr/4.0.0-ALPHA/apache-solr-4.0.0-ALPHA.tgz
    tar zxvf apache-solr-4.0.0-ALPHA.tgz
fi

cp -vr $src_dir/example $dir
rm -rf $dir/{multicore,solr,README.txt}
# mkdir $dir/yokozuna
cp -v solr.xml $dir
