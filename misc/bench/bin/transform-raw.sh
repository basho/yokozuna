#!/bin/bash
#
#> Usage:
#>
#>    ./transform-raw.sh <script dir> <bench results dir> <bench name>

usage() {
    grep '#>' $0 | sed 's/#>//' | sed '$d'
}

if [ ! $# -eq 3 ]; then
    echo "incorrect number of arguments"
    usage
    exit 1
fi

SCRIPT_DIR=$1; shift
BENCH_RESULTS_DIR=$1; shift
BENCH_NAME=$1; shift

RUN_DIR=$BENCH_RESULTS_DIR/$BENCH_NAME

for awk_script_path in $SCRIPT_DIR/*.awk
do
    awk_script=$(basename $awk_script_path)
    metric=${awk_script%-transform.awk}
    src=$RUN_DIR/*${metric}*raw.txt

    for raw in $src; do
        tgt=${raw/-raw.txt/.csv}
        echo "transforming $raw to $tgt"
        if ! gawk -f $awk_script_path $raw > $tgt; then
            echo "failed to transform $src"
            exit 1
        fi
    done
done

gsed -i'' 's/ //g' $RUN_DIR/summary.csv

for lat_file in $RUN_DIR/*latencies.csv; do
    gsed -i'' 's/ //g' $lat_file
done
