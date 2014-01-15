#!/bin/bash
#
# Use to calculate the mean throughput for a particular run of basho
# bench.
#
# ./calc-mean-thru.sh <results-dir>
#
# for d in yz-fruit-query-*; do ./calc-mean-thru.sh $d; done

RESULTS_DIR=$1

echo -n "Mean ops/s for $RESULTS_DIR: "
# Don't include the header, first 3 results or last result
sed -e '1,4d' -e '$d' $RESULTS_DIR/summary.csv | \
    awk -F, '{ secs += $2; ops += $4 } END { printf("%f\n", ops / secs) }'

