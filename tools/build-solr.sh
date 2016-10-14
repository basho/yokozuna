#! /bin/sh
#
# Build Solr from source, applying local patches.
#
#> Usage:
#>
#>   ./build-solr.sh [--git] [--patch-dir <PATCH DIR>] <WORK DIR> <NAME> <URL>
#>
#> Example:
#>
#>   ./build-solr.sh --patch-dir ~/yokozuna/solr-patches /tmp/build-solr solr-4.7.0-yz http://archive.apache.org/dist/lucene/solr/4.7.0/solr-4.7.0-src.tgz | tee build-solr.out

set -e

error()
{
    echo "ERROR: $1"
    exit 1
}

usage()
{
    grep "#>" $0 | sed -e 's/#>//' -e '$d'
}

apply_patches()
{
    if test -e $PATCH_DIR; then
        echo "applying patches in $PATCH_DIR"
        for p in $PATCH_DIR/*.patch; do
            patch -p1 < $p
        done
    fi
}

download()
{
    if which wget > /dev/null; then
        wget --no-check-certificate --progress=dot:mega $1
    elif which curl > /dev/null; then
        curl --insecure --progress-bar -O $1
    fi
}

IS_GIT=0
PATCH_DIR=""
while test $# -gt 0
do
    case $1 in
        --git)
            # <URL> is a git URL
            IS_GIT=1
            ;;
        --patch-dir)
            PATCH_DIR=$2
            # make absolute
            cd $PATCH_DIR
            PATCH_DIR=$(pwd)
            cd -
            shift
            ;;
        -*)
            error "unrecognized option: $1"
            ;;
        *)
            break
            ;;
    esac
    shift
done

if test $# != 3; then
    echo "ERROR: incorrect number of arguments: $#"
    usage
    exit 1
fi

WORK_DIR=$1; shift
NAME=$1; shift
URL=$1; shift

if ! javac -version 2>&1 | egrep "1\.6\.[0-9_.]+"
then
    echo "JDK 1.6 must be used to compile Solr"
    exit 1
fi

if [ ! -x "`which ant`" ]; then
  echo "Couldn't find ant, which is needed to compile Solr."
  exit 1
fi

if test ! -e $WORK_DIR; then
    mkdir $WORK_DIR
fi

cd $WORK_DIR
# make absolute if not already
WORK_DIR=$(pwd)

if test $IS_GIT -eq 1; then
    echo "FIKE"
    SOLR_DIR=$(basename $URL)
    git clone $URL
else
    SOLR_FILE=$(basename $URL)
    SOLR_DIR=${SOLR_FILE%-src.tgz}

    if test ! -e $SOLR_FILE; then
        download $URL
    fi

    if test ! -e $SOLR_DIR; then
        tar zxvf $SOLR_FILE
    fi
fi

mv $SOLR_DIR $NAME
SOLR_DIR=$NAME

cd $SOLR_DIR
echo "buildling Solr from $SOLR_DIR"

apply_patches
ant ivy-bootstrap
ant compile

cd solr
# NOTE: needed for 4.0 release
# mkdir test-framework/lib
ant dist example

cd ..
mv solr $NAME
tar zcvf $NAME.tgz \
    --exclude='build*' \
    --exclude=cloud-dev \
    --exclude=core \
    --exclude=package \
    --exclude=scripts \
    --exclude=site \
    --exclude=solrj \
    --exclude=test-framework \
    --exclude=testlogging.properties \
    --exclude=example/etc/solrtest.keystore \
    $NAME
mv $NAME solr
