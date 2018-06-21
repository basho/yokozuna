# -*- Bash -*-


declare SOLR_VSN=7.3.1

declare YZ_JAR_VSN=3.1S
declare YZ_JAR_NAME=yokozuna-$YZ_JAR_VSN.jar
declare YZ_JAR_SHA=$YZ_JAR_NAME.sha

declare MON_JAR_VSN=1S
declare MON_JAR_NAME=yz_monitor-$MON_JAR_VSN.jar
declare MON_JAR_SHA=$MON_JAR_NAME.sha

function mk_sha()
{
    file=$1
    sha_file=$2

    if type sha1sum &>/dev/null; then
        sha1sum $file > $sha_file
    elif type shasum &>/dev/null; then
        shasum -a 1 $file > $sha_file
    else
        echo "Unable to locate program to compute SHA1"
        exit 1
    fi
}

download()
{
    if which fetch > /dev/null; then
        fetch $1
    elif which wget > /dev/null; then
        wget --progress=dot:mega $1
    elif which curl > /dev/null; then
        curl --progress-bar -O $1
    fi
}

