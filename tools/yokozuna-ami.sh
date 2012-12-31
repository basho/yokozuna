#!/bin/bash
#
# This script builds a Riak/Yokozuna AMI by running on an existing
# instance created by build-ami.sh.  All actions should be idempotent
# in case of failure part way through.
#
# NOTE: This script assumes an Amazon Linux instance.

AMI_NAME=$1; shift
BUCKET=$1; shift
AWS_USER_ID=$1; shift
AWS_ACCESS_KEY_ID=$1; shift
AWS_SECRET_KEY=$1; shift

info() {
    echo INFO: $1
}

error() {
    echo ERROR: $1
    exit 1
}

if_failed() {
    if [ ! "0" == $(echo $?) ]; then
        error "$1"
    fi
}

cd ~/

sudo yum -y install git gcc gcc-c++ make ncurses ncurses-devel openssl openssl-devel java-1.6.0-openjdk-devel

maybe_get() {
    dir=$1
    url=$2
    base=$(basename $url)

    if [ ! -e $dir ]; then
        rm -f $base
        info "grabbing $url"
        wget $url
        tar zxf $base
        info "$base extracted"
    fi
}

# install Ant
maybe_get apache-ant-1.8.4 http://www.reverse.net/pub/apache//ant/binaries/apache-ant-1.8.4-bin.tar.gz
export PATH=~/apache-ant-1.8.4/bin:$PATH

# install Ivy
maybe_get apache-ivy-2.3.0-rc1  http://apache.cs.utah.edu//ant/ivy/2.3.0-rc1/apache-ivy-2.3.0-rc1-bin.tar.gz
cp apache-ivy-2.3.0-rc1/ivy-2.3.0-rc1.jar apache-ant-1.8.4/lib/

# install Erlang, build on ephemeral to avoid out-of-disk
sudo chown ec2-user /media/ephemeral0
sudo chgrp ec2-user /media/ephemeral0
pushd /media/ephemeral0
maybe_get otp_src_R15B02 http://www.erlang.org/download/otp_src_R15B02.tar.gz

if [ ! -d /usr/local/erlang* ]; then
    pushd otp_src_R15B02
    ./configure --prefix=/usr/local/erlangR15B02
    make
    sudo make install
    popd
fi
popd
export PATH=/usr/local/erlangR15B02/bin:$PATH

# build Riak/Yokozuna
if [ ! -d riak ]; then
    git clone git://github.com/basho/riak.git
fi

pushd riak

if [ ! -d rel/riak ]; then
    git checkout rz-yokozuna-2
    make deps
    (cd deps && rm -rf riak_kv && git clone git://github.com/basho/riak_kv.git && cd riak_kv && git checkout rz-yokozuna-3)
    make
    make stage
fi

# write data to ephemeral storage
#
# TODO: move yz data, currently it isn't easy to change yokozuna's
#       working dir
DATA_DIR=/media/ephemeral0/data
sed -i.bak \
    -e "s:\./data/bitcask:$DATA_DIR/bitcask:" \
    -e "s:\./data/leveldb:$DATA_DIR/leveldb:" \
    rel/riak/etc/app.config

popd


# set IO scheduler
GRUB=/etc/grub.conf
if grep 'elevator=' $GRUB; then
    sudo su -c "sed -i.bak 's/elevator=.*/elevator=noop/' $GRUB" - root
else
    sudo cp $GRUB $GRUB.bak
    sudo su -c "echo 'elevator=noop' >> $GRUB" - root
fi

if ! grep 'elevator=' $GRUB; then
    error "failed to set elevator"
fi

# set swappiness
SYSCTL=/etc/sysctl.conf
if grep 'vm.swappiness.*' $SYSCTL; then
    sudo su -c "sed -i.bak 's/vm.swappiness.*/vm.swappiness = 0/' $SYSCTL" - root
else
    sudo cp $SYSCTL $SYSCTL.bak
    sudo su -c "echo 'vm.swappiness = 0' >> $SYSCTL" - root
fi

if ! grep 'vm.swappiness.*' $SYSCTL; then
    error "failed to set swappiness"
fi

# set sshd root login
SSHCFG=/etc/ssh/sshd_config
if sudo grep 'PermitRootLogin.*' $SSHCFG; then
    sudo su -c "sed -i.bak 's/PermitRootLogin.*/PermitRootLogin without-password/' $SSHCFG" - root
else
    sudo cp $SSHCFG $SSHCFG.bak
    sudo su -c "echo 'PermitRootLogin without-password' >> $SSHCFG" - root
fi

if ! sudo grep '^PermitRootLogin without-password$' $SSHCFG; then
    error "failed to set PermitRootLogin"
fi

# add code to fetch users public key and add to authorized_keys
if ! grep 'fetch public key' /etc/rc.local; then
    sudo cp /etc/rc.local /etc/rc.local.bak

    sudo su -c "cat <<'EOF' > /etc/rc.local
    if [ ! -d /root/.ssh ] ; then
        mkdir -p /root/.ssh
        chmod 700 /root/.ssh
    fi

    if [ ! -d /home/ec2-user/.ssh ] ; then
        mkdir -p /home/ec2-user/.ssh
        chmod 700 /home/ec2-user/.ssh
    fi

    # fetch public key using HTTP
    curl http://169.254.169.254/latest//meta-data/public-keys/0/openssh-key > /tmp/my-key
    if [ $? -eq 0 ] ; then
        cat /tmp/my-key >> /root/.ssh/authorized_keys
        cat /tmp/my-key >> /home/ec2-user/.ssh/authorized_keys
        chmod 700 /root/.ssh/authorized_keys
        chmod 700 /home/ec2-user/.ssh/authorized_keys
        rm /tmp/my-key
    fi
EOF" - root
fi

if ! grep 'fetch public key' /etc/rc.local; then
    error "failed to modify rc.local"
fi

# up ulimit
LIMITS=/etc/security/limits.conf
if ! grep 'ec2-user.*nofile.*700000' $LIMITS; then
    sudo su -c "echo 'ec2-user - nofile 700000' >> $LIMITS" - root
fi

if ! grep 'ec2-user.*nofile.*700000' $LIMITS; then
    error "failed to raise ulimit"
fi

# clear any history
sudo find /root/.*history /home/*/.*history -exec rm -f {} \;

# build/publish the AMI
pushd /media/ephemeral0
BUNDLE_DIR=/media/ephemeral0/bundle

rm -vrf $BUNDLE_DIR
mkdir -v $BUNDLE_DIR

sudo su -c "cd /media/ephemeral0 && ec2-bundle-vol -k pk-* -c cert-* -r x86_64 -d $BUNDLE_DIR -e /home/ec2-user/.ssh/authorized_keys -e /etc/ssh/ssh_host_dsa_key -e /etc/ssh/ssh_host_dsa_key.pub -e /etc/ssh/ssh_host_key -e /etc/ssh/ssh_host_key.pub -e /etc/ssh/ssh_host_rsa_key -e /etc/ssh/ssh_host_rsa_key.pub -p $AMI_NAME -u $AWS_USER_ID" - root
if_failed "failure bundling volume"

sudo su -c "cd /media/ephemeral0 && ec2-upload-bundle -b $BUCKET -m $BUNDLE_DIR/image.manifest.xml -a $AWS_ACCESS_KEY_ID -s $AWS_SECRET_KEY" - root
if_failed "failure uploading volume"

ec2-register -K pk-* -C cert-* $BUCKET/image.manifest.xml -n $AMI_NAME
if_failed "failure registering ami"

popd
