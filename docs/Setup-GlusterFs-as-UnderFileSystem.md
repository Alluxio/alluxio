---
layout: global
title: Setup GlusterFs as UnderFileSystem
---

Tachyon can use [GlusterFs](http://www.gluster.org) as its UnderFileSystem.

# Prerequisites

You need to install GlusterFS on your cluster and create a GlusterFS volume
[GlusterFS usage](http://www.gluster.org/community/documentation/index.php/QuickStart).

Compile Tachyon with GlusterFS: `mvn clean install -P glusterfs -DGlusterfsTest -Dhadoop.version=2.3.0`

# Mount a GlusterFS filesystem

Assume the GlusterFS bricks are co-located with Tachyon nodes, the GlusterFS volume name is `gsvol`,
and the mount point is `/vol`.

On each Tachyon node, edit `/etc/fstab` and add `localhost:gsvol /vol glusterfs`. Then mount the
GlusterFS filesystem:

    $ mount -a -t glusterfs

# Configure Tachyon to use GlusterFS filesystem

Next, config Tachyon in `tachyon` folder:

    $ cd /root/tachyon/conf
    $ cp tachyon-glusterfs-env.sh.template tachyon-env.sh

Add the following lines to the `tachyon-env.sh` file:

	TACHYON_UNDERFS_GLUSTER_VOLUMES=tachyon_vol
	TACHYON_UNDERFS_GLUSTER_MOUNTS=/vol

Sync the configuration to all nodes.

# Format the filesystem

    $ cd /root/tachyon/
    $ ./bin/tachyon format

# Verify that GlusterFS is ready to use

    $ cd /vol
    $ ls

You should find that a tachyon folder has been created.
