#!/bin/bash
#
# Collect CPU and MEM metrics for specific processes.

ACTION=$1; shift
output=/tmp/pid-cpu-mem-collect-raw.txt

case $ACTION in
    start)
        pkill prstat
        prstat -du -p "$(pgrep -d, 'beam|java')" 1 > $output
        ;;
    stop)
        pkill prstat
        ;;
    output)
        echo $output
        ;;
esac
