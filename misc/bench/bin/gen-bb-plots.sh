#!/bin/bash
#
# Generate the BB summary plots for a set of runs.
#
# ./gen-bb-plots.sh <bb dir> <dir>

BB_DIR=$1; shift
DIR=$1; shift

for d in $DIR/*; do
    name=$(basename $d)
    out_file=$d/$name.png
    echo "generating summary $out_file"
    $BB_DIR/priv/summary.r -i $d -o $d/$name.png
done
