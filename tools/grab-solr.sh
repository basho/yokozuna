#!/usr/bin/env bash
#
# Script to grab Solr and embed in priv dir.
#
# Usage:
#     ./grab-solr.sh

SRC="bin-tar"

if [ $(basename $PWD) != "tools" ]
then
    cd tools
fi

while [ $# -gt 0 ]
do
    case $1 in
        git)
            SRC="git"
            shift
            ;;
        src-tar)
            SRC="src-tar"
            shift
            ;;
        *)
            echo Invalid argument $1
            exit 1
    esac
done

echo "Create solr dir..."
SOLR_DIR=../priv/solr
BUILD_DIR=../build

if [ $SRC == "git" ]; then
    VSN=lucene-solr
    src_dir=$BUILD_DIR/$VSN
    example_dir=$src_dir/solr/example
    patch_dir=../solr-patches
    branch=branch_4x
elif [ $SRC == "src-tar" ]; then
    VSN=solr-4.1.0-src
    src_dir=$BUILD_DIR/${VSN%-src}
    example_dir=$src_dir/solr/example
    patch_dir=../solr-patches
else
    VSN=solr-4.3.0-yz
    src_dir=$BUILD_DIR/$VSN
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
    echo "Building Solr..."
    pushd $src_dir
    apply_patches
    ant ivy-bootstrap
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
    test -e $SOLR_DIR
}

get_solr()
{
    echo "Getting Solr..."
    if [ $SRC == "git" ]; then
        git clone git://github.com/apache/$VSN.git
    else
        wget https://s3.amazonaws.com/yzami/pkgs/$VSN.tgz
        tar zxf $VSN.tgz
    fi
}

if check_for_solr
then
    echo "Solr already exists, exiting..."
    exit 0
fi

echo "Create build dir..."
if [ ! -e $BUILD_DIR ]; then
    mkdir $BUILD_DIR
fi

cd $BUILD_DIR

if [ ! -e $src_dir ]
then
    get_solr
fi

if [ $SRC == "git" ]; then
    checkout_branch $branch
    build_solr
elif [ $SRC == "src-tar" ]; then
    build_solr
fi

echo "Creating solr dir from Solr example..."
cp -r $example_dir $SOLR_DIR
rm -rf $SOLR_DIR/{cloud-scripts,example-DIH,exampledocs,multicore,logs,solr,README.txt,logging.properties}
cp ../priv/solr.xml $SOLR_DIR
cp ../priv/jetty.xml $SOLR_DIR/etc
cp ../priv/*.properties $SOLR_DIR


echo "Finished creating solr dir..."
