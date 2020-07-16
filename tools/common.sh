# -*- Bash -*-


readonly SOLR_VSN=7.3.1

readonly YZ_JAR_VSN=3.2
readonly YZ_JAR_NAME=yokozuna-$YZ_JAR_VSN.jar
readonly YZ_JAR_SHA=$YZ_JAR_NAME.sha

readonly MON_JAR_VSN=1.2
readonly MON_JAR_NAME=yz_monitor-$MON_JAR_VSN.jar
readonly MON_JAR_SHA=$MON_JAR_NAME.sha

mk_sha()
{
    file=$1
    sha_file=$2

    if type sha512t256 &>/dev/null; then
        sha512t256 $file > $file.sha512
    elif type sha512sum &>/dev/null; then
        sha512sum $file > $file.sha512
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
        wget --no-check-certificate --progress=dot:mega $1
    elif which curl > /dev/null; then
        curl --progress-bar -O $1
    fi
}

