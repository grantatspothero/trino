#!/usr/bin/env bash

set -exuo pipefail

# create local device
dd if=/dev/zero of=/opt/ssd0 bs=1M count=10240
mknod /dev/fakessd0 b 7 200
# deleting the device if exists, ignoring the error if it doesn't
losetup -d /dev/fakessd0 || true
losetup /dev/fakessd0 /opt/ssd0
dd if=/dev/fakessd0 of=/tmp/page count=1 bs=8K
echo "8388608" > /proc/sys/fs/aio-max-nr
