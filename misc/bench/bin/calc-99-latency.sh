#!/bin/bash
#
# Use to calculate the 99th percentile latency for a particular run of
# basho bench.
#
# ./calc-99-latency.sh <results-dir>
#
# for d in bench-runs-dir/*; do ./calc-99-latency.sh $d; done

RESULTS_DIR=$1

for lat in $RESULTS_DIR/*latencies*
do
    # taking average of all the 99th percentiles, divide by 1000 to
    # convert from microseconds to milli
    #
    # drop first 30 seconds and last line to remove outliers
    echo -n "The mean 99th latency for $lat: "
    sed -e '1,4d' -e '$d' $lat | \
        awk -F, '{total += $8 } END { printf("%f\n", (total / NR) / 1000) }'
done
