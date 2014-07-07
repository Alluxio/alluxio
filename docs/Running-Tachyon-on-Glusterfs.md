---
layout: global
title: Running Tachyon on Glusterfs
---

Tachyon can use Glusterfs [Glusterfs](http://www.gluster.org).

# Prerequisites

You need to install Glusterfs on your cluster and create a Glusterfs volume [Glusterfs usage](http://www.gluster.org/community/documentation/index.php/QuickStart).

# Mount a Glusterfs filesystem

Assume the Glusterfs bricks are co-located with Tachyon nodes, the Glusterfs volume name is `gsvol`, and the mount point is `/vol`.

On each Tachyon node, edit `/etc/fstab` and add `localhost:gsvol   /vol   glusterfs`, then mount the Glusterfs filesystem
    $ mount -a -t glusterfs

# Configure Tachyon to use Glusterfs filesystem

Next, config Tachyon in `tachyon` folder

    $ cd /root/tachyon/conf
    $ cp tachyon-glusterfs-env.sh.template tachyon-env.sh

Edit `tachyon-env.sh` file, add `TACHYON_UNDERFS_GLUSTER_VOLUMES=tachyon_vol` and `TACHYON_UNDERFS_GLUSTER_MOUNTS=/vol`.

Sync the configuration to all nodes.

# Format filesystem

    $ cd /root/tachyon/
    $ ./bin/tachyon format

# Verify Glusterfs is ready to use

    $ cd /vol
    $ ls 

You should find tachyon folder are created.