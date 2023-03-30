#!/bin/sh
set -e
cat << "EOF" | docker run -i --rm -v $PWD:/data --privileged alpine:3.14
apk --update add e2fsprogs
dd if=/dev/zero of=/data/ext4.img bs=1M count=10
mkfs.ext4 /data/ext4.img
mount /data/ext4.img /mnt
mkdir /mnt/foo
mkdir /mnt/foo/bar
echo "This is a short file" > /mnt/shortfile.txt
dd if=/dev/zero of=/mnt/two-k-file.dat bs=1024 count=2
dd if=/dev/zero of=/mnt/six-k-file.dat bs=1024 count=6
dd if=/dev/zero of=/mnt/seven-k-file.dat bs=1024 count=7
dd if=/dev/zero of=/mnt/ten-meg-file.dat bs=1M count=10
i=0; until [ $i -gt 10000 ]; do mkdir /mnt/foo/dir${i}; i=$(( $i+1 )); done
umount /mnt
EOF
