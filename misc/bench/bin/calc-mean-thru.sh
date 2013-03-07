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
# Don't include the header, 1st result or last result
sed -e '1,2d' -e '$d' $RESULTS_DIR/summary.csv | \
    awk -F, 'NR != 1 { total += $4/$2 } END { printf("%f\n", total / NR) }'
