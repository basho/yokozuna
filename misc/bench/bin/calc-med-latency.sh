#!/bin/bash
#
# Use to calculate the median latency for a particular run of basho
# bench.
#
# ./calc-median-latency.sh <results-dir>
#
# for d in yz-fruit-query-*; do ./calc-median-latency.sh $d; done

RESULTS_DIR=$1

for lat in $RESULTS_DIR/*latencies*
do
    # taking average of all the medians, divide by 1000 to convert
    # from microseconds to milli
    echo -n "The mean median latency for $lat: "
    sed -e '1,2d' -e '$d' $lat | \
        awk -F, '{total += $6 } END { printf("%f\n", (total / NR) / 1000) }'
done
