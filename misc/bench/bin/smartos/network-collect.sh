#!/bin/bash
#
# Collect network metrics
#
# This relies on building nicstat from source.
#
# See: http://www.brendangregg.com/K9Toolkit/nicstat.c

ACTION=$1
output=/tmp/network-collect-raw.txt

case $ACTION in
    start)
        pkill nicstat
        ~/k9/nicstat 1 > $output
        ;;
    stop)
        pkill nicstat
        ;;
    output)
        echo $output
        ;;
esac
