#!/bin/bash
#
# The main script for building a custom AMI.  The actual customization
# and AMI bundle creation is performed in a second script which is
# passed as BUILD_SCRIPT and runs locally on the build instance.
#
#> Usage:
#>    build-ami.sh CLIENT_TOKEN BASE_AMI AMI_NAME YZ_COMMIT BUILD_SCRIPT KEY_PAIR KEY_PAIR_FILE AWS_USER_ID S3_BUCKET
#>
#>    CLIENT_TOKEN - An identifier used for ec2 idempotency.  If this script
#>                   fails part way through the token will make sure the
#>                   process is resumed on the same instance.
#>
#>    BASE_AMI - The base AMI to be customized.
#>
#>    AMI_NAME - The name to register the custom AMI under.
#>
#>    YZ_COMMIT - Currently unused.
#>
#>    BUILD_SCRIPT- Path to the script that will customize the AMI, bundle
#>                  the volume, upload it, and register it.
#>
#>    KEY_PAIR - The name of the ec2 key pair to start the BASE_AMI with.
#>
#>    KEY_PAIR_FILE - The path to the private key for KEY_PAIR.  Used to
#>                    login to the BASE_AMI instance.
#>
#>    AWS_USER_ID - Needed to bundle the AMI.
#>
#>    S3_BUCKET - The S3 bucket to upload the custom AMI too.
#>
#> Example:
#>    ./build-ami.sh yokozuna_build ami-f565ba9c yokozuna_ami master yokozuna-ami.sh \
#>                   ec2key ~/.ec2/id_rsa-ec2key 111111111111 mybucket

usage() {
    echo
    grep '#>' $0 | tr -d '#>' | sed '$d'
}

info() {
    echo INFO: $1
}

error() {
    echo ERROR: $1
    usage
    exit 1
}

if_failed() {
    if [ ! "0" == $(echo $?) ]; then
        error "$1"
    fi
}

if [ ! $# -eq 9 ]; then
    error "incorrect number of arguments"
fi

# Used for idempotent creation of build instance
CLIENT_TOKEN=$1; shift
BASE_AMI=$1; shift
AMI_NAME=$1; shift
YZ_COMMIT=$1; shift
BUILD_SCRIPT=$1; shift
KEY_PAIR=$1; shift
KEY_PAIR_FILE=$1; shift
AWS_USER_ID=$1; shift
S3_BUCKET=$1; shift

check() {
    if [ -z $EC2_PRIVATE_KEY ]; then
        error "the env var EC2_PRIVATE_KEY must be set"
    fi

    if [ -z $EC2_CERT ]; then
        error "the env var EC2_CERT must be set"
    fi

    if [ -z $AWS_ACCESS_KEY ]; then
        error "the env var AWS_ACCESS_KEY must be set"
    fi

    if [ -z $AWS_SECRET_KEY ]; then
        error "the env var AWS_SECRET_KEY must be set"
    fi

    if ! which ec2-run-instances 1> /dev/null; then
        error "ec2 tools not found"
    fi

    if ! ec2-describe-images $BASE_AMI; then
        error "invalid ami: $BASE_AMI"
    fi

    if [ "1" == $(ec2-describe-images --filter name=$AMI_NAME | wc -l | tr -d ' ') ]; then
        error "AMI with name $AMI_NAME already exists"
    fi
}

start_instance() {
    base_ami=$1
    key_pair=$2
    client_token=$3

    ec2-run-instances $base_ami -t m1.small -k $key_pair --client-token $client_token
}

wait_for_instance() {
    client_token=$1; shift
    num_tries=0

    info "waiting for instance with client-token $client-token"
    while [ ! "running" == "$(ec2-describe-instances --filter client-token=$client_token | sed -n '2p' | awk '{ print $6 }')" ]; do
        num_tries=$((num_tries + 1))
        if [ $num_tries -eq 60 ]; then
            echo
            error "instance is not in 'running' status after 5 minutes"
        fi
        sleep 5s
        echo -n "."
    done

    echo OK

}

wait_for_sshd() {
    instance_host=$1; shift
    key_pair_file=$1; shift

    num_tries=0
    info "waiting for sshd on $instance_host to accept"
    while ! ssh -i $key_pair_file -o StrictHostKeyChecking=no ec2-user@$instance_host 'echo ohai'; do
        num_tries=$((num_tries +1))
        if [ $num_tries -eq 36 ]; then
            error "sshd is not up on $instance_host after 3 minutes"
        fi
        sleep 5s
    done
}

set_instance_data() {
    INSTANCE_ID=$(ec2-describe-instances --filter client-token=$client_token | sed -n '2p' | awk '{ print $2 }')
    INSTANCE_HOST=$(ec2-describe-instances --filter client-token=$client_token | sed -n '2p' | awk '{ print $4 }')
    info "instance id: $INSTANCE_ID instance host: $INSTANCE_HOST"
}

copy_build_script() {
    instance_host=$1
    key_pair_file=$2
    build_script=$3

    info "copying build script $build_script to host $instance_host"
    scp -i $key_pair_file \
        -o StrictHostKeyChecking=no \
        $build_script ec2-user@$instance_host:~/
    if_failed "failure copying build script"

}

run_build_script() {
    instance_host=$1; shift
    key_pair_file=$1; shift
    build_script=$1; shift

    basename=$(basename $build_script)
    ssh -t -i $key_pair_file \
        -o StrictHostKeyChecking=no \
        ec2-user@$instance_host "chmod u+x $basename; ./$basename $@"
}

copy_creds() {
    instance_host=$1
    key_pair_file=$2

    info "copying credentials"
    scp -i $key_pair_file \
        -o StrictHostKeyChecking=no \
        $EC2_PRIVATE_KEY $EC2_CERT ec2-user@$instance_host:/media/ephemeral0
    if_failed "failure copying credentials"
}

change_perms() {
    instance_host=$1; shift
    key_pair_file=$1; shift

    ssh -t -i $key_pair_file \
        -o StrictHostKeyChecking=no \
        ec2-user@$instance_host "sudo chown ec2-user /media/ephemeral0 && sudo chgrp ec2-user /media/ephemeral0"
    if_failed "failure changing permissions"
}

check
start_instance $BASE_AMI $KEY_PAIR $CLIENT_TOKEN
wait_for_instance $CLIENT_TOKEN
set_instance_data
wait_for_sshd $INSTANCE_HOST $KEY_PAIR_FILE
change_perms $INSTANCE_HOST $KEY_PAIR_FILE
copy_build_script $INSTANCE_HOST $KEY_PAIR_FILE $BUILD_SCRIPT
copy_creds $INSTANCE_HOST $KEY_PAIR_FILE
run_build_script $INSTANCE_HOST $KEY_PAIR_FILE $BUILD_SCRIPT \
    $AMI_NAME $S3_BUCKET $AWS_USER_ID $AWS_ACCESS_KEY $AWS_SECRET_KEY
