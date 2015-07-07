#!/bin/bash

# To make sure all external storages specified in block device mapping are mounted with ext4

# DeviceName specified in block device mapping can not be trusted
# they may be changed by kernel driver
# e.x. /dev/sdb may be changed into /dev/xvdb, or /dev/sdh, even /dev/hdh
# the behavior depends on what type of virtualization you use(PV/HVM), your AMI, your instance type
# And some AMI may mount devices defined in block device mapping by default, others won't
# for more info, visit http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/block-device-mapping-concepts.html
# So, it's not as trivial as just iterating over DeviceNames, format the disks and mount them
# Also, We can not just check whether devicenames can be found in `df` to determine whether they have been mounted
# because devicenames may be changed silently by kernel

# But user specified devicenames should be defined according to
#  http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html
# they should start with /dev/sd* or /dev/xvd* or /dev/hd*, so this script will view items matching one of these
# three patterns and not ending in "da"(this is revserved by default) as devices specified in block device mapping
# Next, determine those of them that are not mounted yet

# Solution to determine the devices that haven't been mounted is:
# find those in the set of the user specified devices but not in the results of `df` yet
dev=`ls /dev | egrep '^sd|^hd|^xvd' | grep -v da`
echo "possible devices defined in block device mapping: "
printf "%s\n" $dev

mounted=`df | cut -d' ' -f1 | grep ^/dev | grep -v da | sed -r 's/^.{5}//'`
echo "devices in /dev that are already mounted: "
printf "%s\n" $mounted

# trick to implement set complement, will output items in $dev but not in $mounted
to_mount=`printf "%s\n" $mounted $mounted $dev | sort | uniq -u`
echo "devices known by kernel but not mounted yet: "
printf "%s\n" $to_mount

# format disk, sequentially mount to /disk0, /disk1, ...
n=0
for disk in $to_mount; do
 sudo mkfs.ext4 "/dev/$disk"
 sudo mkdir "/disk$n"
 sudo mount "/dev/$disk" "/disk$n"
 sudo chown -R `whoami` "/disk$n"
 n=$(( $n + 1 ))
done
echo "$n devices mounted as /disk#"
