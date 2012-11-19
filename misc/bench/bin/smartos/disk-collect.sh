#!/bin/bash
#
# Collect disk metrics.

ACTION=$1

output=/tmp/disk-collect-raw.txt

case $ACTION in
    start)
        pkill iostat
        iostat -rsxTu cmdk0 cmdk1 1 > $output
        ;;
    stop)
        pkill iostat
        ;;
    output)
        echo $output
        ;;
esac
