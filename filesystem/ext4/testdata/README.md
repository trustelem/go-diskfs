# ext4 Test Fixtures
This directory contains test fixtures for ext4 filesystems. Specifically, it contains the following files:

* `ext4.img`: A 100MB filesystem img

Because of the size of the image, it is excluded from git. It needs to be generated anew for each
installation on which you want to test. Of course, each generation can give slightly different
inode information, and certainly will give different timestamps, so you need to update the tests
appropriately; see below.

To generate the `ext4.img`, run `./buildimg.sh`.

This makes:

* the `/foo` directory with sufficient entries to require using hash tree directories
* some small and large files in the root

You now have the exact files in `$PWD`
