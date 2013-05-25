#!/bin/bash
#
# Use to calculate the 95th latency for a particular run of basho
# bench.
#
# ./calc-95-latency.sh <results-dir>
#
# for d in query-*; do ./calc-95-latency.sh $d; done

RESULTS_DIR=$1

for lat in $RESULTS_DIR/*latencies*
do
    # taking average of all the medians, divide by 1000 to convert
    # from microseconds to milli
    echo -n "The mean 95th latency for $lat: "
    sed -e '1,2d' -e '$d' $lat | \
        awk -F, '{total += $7 } END { printf("%f\n", (total / NR) / 1000) }'
done
