#!/bin/bash
#
#> Usage:
#>
#>    ./run-bench.sh <user> <hosts file> <key> <script dir> <bench root> <bench config> <bench results dir> <bench name>

usage() {
    grep '#>' $0 | sed 's/#>//' | sed '$d'
}

error() {
    echo $1
    exit 1
}

if [ ! $# -eq 8 ]; then
    echo "incorrect number of arguments"
    usage
    exit 1
fi

USER=$1; shift
HOSTS_FILE=$1; shift
KEY=$1; shift
SCRIPT_DIR=$1; shift
BENCH_ROOT=$1; shift
BENCH_CFG=$1; shift
BENCH_RESULTS_DIR=$1; shift
BENCH_NAME=$1; shift

RUN_DIR=$BENCH_RESULTS_DIR/$BENCH_NAME

verify_hosts() {
    while read host; do
        user_host=$USER@$host
        if ssh -fi $KEY $user_host 'exit'; then
            echo "verified login $user_host"
        else
            error "failed to login $user_host"
        fi
    done < $HOSTS_FILE
}

copy_collect_scripts() {
    while read host; do
        user_host=$USER@$host
        if ! scp -i $KEY -r $SCRIPT_DIR/*collect.sh $user_host:~/; then
            error "failed to copy collection scripts to $user_host"
        fi
    done < $HOSTS_FILE
}

start_collecting() {
    while read host; do
        user_host=$USER@$host
        for script in $SCRIPT_DIR/*collect.sh; do
            script=$(basename $script)
            ssh -fi $KEY $user_host "chmod a+x $script && ./$script start"
        done
    done < $HOSTS_FILE
}

stop_collecting() {
    while read host; do
        user_host=$USER@$host
        for script in $SCRIPT_DIR/*collect.sh; do
            script=$(basename $script)
            ssh -fi $KEY $user_host "./$script stop"
        done
    done < $HOSTS_FILE
}

copy_output() {
    while read host; do
        user_host=$USER@$host
        for script in $SCRIPT_DIR/*collect.sh; do
            output_path=$($script output)
            output_file=$(basename $output_path)
            src=$user_host:$output_path
            dest=$RUN_DIR/${user_host}-${output_file}
            echo "copying output from $src to $dest"
            scp -qi $KEY $src $dest
        done
    done < $HOSTS_FILE
}

run_bench() {
    $BENCH_ROOT/basho_bench -d $BENCH_RESULTS_DIR -n $BENCH_NAME $BENCH_CFG
}

verify_hosts
copy_collect_scripts
start_collecting
run_bench
sleep 10s
stop_collecting
copy_output
