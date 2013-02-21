#!/bin/bash
#
#> Usage:
#>
#>    ./make-vis.sh <script dir> <bench results dir> <bench name> <www dir>

usage() {
    grep '#>' $0 | sed 's/#>//' | sed '$d'
}

if [ ! $# -eq 4 ]; then
    echo "incorrect number of arguments"
    usage
    exit 1
fi

SCRIPT_DIR=$1; shift
BENCH_RESULTS_DIR=$1; shift
BENCH_NAME=$1; shift
WWW_DIR=$1; shift

RUN_DIR=$BENCH_RESULTS_DIR/$BENCH_NAME
WWW_DIR=$WWW_DIR/$BENCH_NAME

mkdir -p $WWW_DIR
cp -vr $RUN_DIR/* $WWW_DIR
cp -v $SCRIPT_DIR/visualize.html $WWW_DIR
cp -vr $SCRIPT_DIR/js $WWW_DIR

add_script() {
    script=$1

    gsed -i'' "/<!-- generate scripts here -->/ a\
$script\n" $WWW_DIR/visualize.html
}

for lat in $RUN_DIR/*_latencies.csv; do
    file=$(basename $lat)
    script="<script>init_latency(\"$file\")</script>"
    add_script $script
done

disks=""
for disk in $RUN_DIR/*-disk-collect.csv; do
    file=$(basename $disk)
    name=${file%-disk-collect.csv}
    disks="{name:\"$name\",resource:\"$file\"},$disks"
done
add_script "<script>init_disks([${disks%,}],\"%b\",\"db\",\"Disk %b\",\"absolute\")</script>"
add_script "<script>init_disks([${disks%,}],\"kr/s\",\"dkrs\",\"Disk kr/s\",\"relative\")</script>"
add_script "<script>init_disks([${disks%,}],\"kw/s\",\"dkws\",\"Disk kw/s\",\"relative\")</script>"

cpus=""
for cpu in $RUN_DIR/*-pid-cpu-mem-collect.csv; do
    file=$(basename $cpu)
    name=${file%-pid-cpu-mem-collect.csv}
    cpus="{name:\"$name\",resource:\"$file\"},$cpus"
done
add_script "<script>init_cpus([${cpus%,}])</script>"

nics=""
for nic in $RUN_DIR/*-network-collect.csv; do
    file=$(basename $nic)
    name=${file%-network-collect.csv}
    nics="{name:\"$name\",resource:\"$file\"},$nics"
done
add_script "<script>init_nics([${nics%,}],\"rKb/s\",\"rkbs\",\"Network rKb/s\",\"relative\")</script>"
add_script "<script>init_nics([${nics%,}],\"wKb/s\",\"wkbs\",\"Network wKb/s\",\"relative\")</script>"
add_script "<script>init_nics([${nics%,}],\"rPk/s\",\"rpks\",\"Network rPk/s\",\"relative\")</script>"
add_script "<script>init_nics([${nics%,}],\"wPk/s\",\"wpks\",\"Network wPk/s\",\"relative\")</script>"
add_script "<script>init_nics([${nics%,}],\"rAvs\",\"ravs\",\"Network rAvs\",\"relative\")</script>"
add_script "<script>init_nics([${nics%,}],\"wAvs\",\"wavs\",\"Network wAvs\",\"relative\")</script>"

open http://localhost/$BENCH_NAME/visualize.html
