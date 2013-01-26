#!/bin/bash
#
# Script to grab Solr and embed in priv dir.
#
# Usage:
#     ./grab-solr.sh

FROM_SRC=false

if [ $(basename $PWD) != "priv" ]
then
    cd priv
fi

while [ $# -gt 0 ]
do
    case $1 in
        from-src)
            FROM_SRC=true
            shift
            ;;
        *)
            echo Invalid argument $1
            exit 1
    esac
done

if $FROM_SRC; then
    VSN=lucene-solr
    dir=$PWD/solr
    src_dir=$PWD/$VSN
    example_dir=$src_dir/solr/example
    patch_dir=$PWD/solr-patches
    branch=branch_4x
else
    VSN=solr-4.1.0
    dir=$PWD/solr
    src_dir=$PWD/$VSN
    example_dir=$src_dir/example
fi

apply_patches()
{
    if [ -e $patch_dir ]; then
        echo "applying patches in $patch_dir"
        for p in $patch_dir/*.patch; do
            patch -p1 < $p
        done
    fi
}

build_solr()
{
    pushd $src_dir
    apply_patches
    ant compile
    pushd solr
    mkdir test-framework/lib
    ant dist example
    popd
    popd
}

checkout_branch()
{
    branch=$1
    pushd $src_dir
    git checkout $branch
    popd
}

check_for_solr()
{
    test -e $dir
}

get_solr()
{
    if $FROM_SRC; then
        git clone git://github.com/apache/$VSN.git
    else
        wget http://s3.amazonaws.com/files.basho.com/solr/$VSN.tgz
        tar zxvf $VSN.tgz
    fi
}

if check_for_solr
then
    echo "Solr already exists, exiting..."
    exit 0
fi

if [ ! -e $src_dir ]
then
    get_solr
fi

if $FROM_SRC; then
    checkout_branch $branch
    build_solr
fi

cp -vr $example_dir $dir
rm -rf $dir/{cloud-scripts,example-DIH,exampledocs,multicore,logs,solr,README.txt}
cp -v solr.xml $dir
cp -v *.properties $dir
