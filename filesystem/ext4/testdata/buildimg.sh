#!/bin/sh
set -e
cat << "EOF" | docker run -i --rm -v $PWD:/data --privileged alpine:3.14
apk --update add e2fsprogs
dd if=/dev/zero of=/data/ext4.img bs=1M count=100
mkfs.ext4 /data/ext4.img
mount /data/ext4.img /mnt
mkdir /mnt/foo
mkdir /mnt/foo/bar
echo "This is a short file" > /mnt/shortfile.txt
echo "Another file" > /mnt/foo/bar/short2.txt
dd if=/dev/zero of=/mnt/two-k-file.dat bs=1024 count=2
dd if=/dev/zero of=/mnt/six-k-file.dat bs=1024 count=6
dd if=/dev/zero of=/mnt/seven-k-file.dat bs=1024 count=7
(i=0; while true; do echo " $i " ; i=$(( $i+1 )); done) | dd of=/mnt/ten-meg-file.dat bs=1M count=10 iflag=fullblock
i=0; until [ $i -gt 10000 ]; do mkdir /mnt/foo/dir${i}; i=$(( $i+1 )); done
umount /mnt
EOF
# echo show_inode_info /ten-meg-file.dat | DEBUGFS_PAGER="cat" debugfs ext4.img -f -