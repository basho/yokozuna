#! /bin/sh
#
# Build Solr from source, applying local patches.
#
#> Usage:
#>
#>   ./build-solr.sh [--git] [--patch-dir <PATCH DIR>] <WORK DIR> <URL>
#>
#> Example:
#>
#>   ./build-solr.sh --patch-dir ~/yokozuna/solr-patches /tmp/build-solr http://www.motorlogy.com/apache/lucene/solr/4.2.0/solr-4.2.0-src.tgz | tee build-solr.out

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

if test $# != 2; then
    echo "ERROR: incorrect number of arguments: $#"
    usage
    exit 1
fi

WORK_DIR=$1; shift
URL=$1; shift

mkdir $WORK_DIR
if test ! -e $WORK_DIR; then
    error "failed to created work dir: $WORK_DIR"
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
        wget $URL
    fi

    if test ! -e $SOLR_DIR; then
        tar zxvf $SOLR_FILE
    fi
fi

cd $SOLR_DIR
echo "buildling Solr from $SOLR_DIR"

apply_patches
ant ivy-bootstrap
ant compile

cd solr
# NOTE: needed for 4.0 release
# mkdir test-framework/lib
ant dist example

# TODO: tar.gz example dir
