#!/bin/bash
#
# Collect virtual memory statistics.

ACTION=$1
output=/tmp/virtual-memory-collect-raw.txt

case $ACTION in
    start)
        pkill vmstat
        vmstat -Tu 1 > $output
        ;;
    stop)
        pkill vmstat
        ;;
    output)
        echo $output
        ;;
esac
