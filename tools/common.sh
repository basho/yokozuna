# -*- Bash -*-


declare SOLR_VSN=7.3.1

declare YZ_JAR_VSN=3.2
declare YZ_JAR_NAME=yokozuna-$YZ_JAR_VSN.jar
declare YZ_JAR_SHA=$YZ_JAR_NAME.sha

declare MON_JAR_VSN=1.2
declare MON_JAR_NAME=yz_monitor-$MON_JAR_VSN.jar
declare MON_JAR_SHA=$MON_JAR_NAME.sha

function mk_sha()
{
    file=$1
    sha_file=$2

    if type sha512t256 &>/dev/null; then
        sha512t256 $file > $file.sha512
    elif type shasum &>/dev/null; then
        shasum -a 512256 $file > $file.sha512
    else
        echo "Unable to locate program to compute SHA"
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

