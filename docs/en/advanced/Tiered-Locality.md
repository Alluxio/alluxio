---
layout: global
title: Tiered Locality
nickname: Tiered Locality
group: Advanced
priority: 1
---

* Table of Contents
{:toc}

Alluxio's tiered locality feature enables applications to make intelligent, topology-based decisions
about which Alluxio workers to read from and write to.
For an application running on `host1`, reading data from an Alluxio worker on `host1` is more efficient 
than reading from a worker on `host2`. Similarly, reading from a worker on the same rack or in the same
datacenter is faster than reading from a worker on a different rack or different datacenter. You can take
advantage of these various levels of locality by configuring your servers and clients with network 
topology information.

## Tiered Identity

In Alluxio, each entity (masters, workers, clients) has a *Tiered Identity*. A tiered
identity is an address tuple such as **`(node=node_name, rack=rack_name, datacenter=datacenter_name)`**.
Each pair in the tuple is called a *locality tier*.
Alluxio clients favor reading from and writing to workers that are "more local". For example, if a client has
identity **`(node=A, rack=rack1, datacenter=dc1)`**, it will prefer to read from a worker at
**`(node=B, rack=rack1, datacenter=dc1)`** over a worker at **`(node=C, rack=rack2, datacenter=dc1)`** because 
node `B` is on the same rack as the client, and node `C` is on a different rack.

## Basic Setup

For this example, suppose your Alluxio workers are spread across multiple availability zones within EC2.
To configure tiered locality, follow these steps:

1. Write a script named `alluxio-locality.sh`. Alluxio uses this script to determine the tiered identity
   for each entity. The output format is a comma-separated list of `tierName=tierValue pairs`.
   ```
   #!/bin/bash
  
   node=$(hostname)
   # Ask EC2 which availability zone we're in
   availability_zone=$(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone)
   
   echo "node=${node},availability_zone=${availability_zone}"
   ```
  
1. Make the script executable with `chmod +x alluxio-locality.sh`.

1. Include the script on the classpath of your applications and Alluxio servers. For servers,
   you can put the file in the `conf` directory.
  
1. On the Alluxio master(s), set `alluxio.locality.order=node,availability_zone` to define the order
   of locality tiers.
  
1. Restart Alluxio servers to pick up the configuration changes.

To verify that the configuration is working, check the master, worker, and application logs. You
should see a line like

```
INFO  TieredIdentityFactory - Initialized tiered identity TieredIdentity(node=ip-xx-xx-xx-xx, availability_zone=us-east-1)
```

If you're having problems, try running your locality script directly to check its output, and make sure 
it is executable by the user running the Alluxio servers.

## Advanced

### Custom locality tiers

By default, Alluxio has two locality tiers: `node` and `rack`. Users may customize the
set of locality tiers to take advantage of more advanced topologies. To change the set
of tiers available, set `alluxio.locality.order` on the master and restart the cluster.
The order should go from most specific to least specific. For example, to add
availability zone locality to a cluster, set

```
alluxio.locality.order=node,rack,availability_zone
```

If the user does nothing to provide tiered identity info, each entity will
perform a localhost lookup to set its node-level identity info. If other locality tiers
are left unset, they will not be used to inform locality decisions. To set
the value for a locality tier, set the configuration property

### Set locality via configuration properties

The `alluxio-locality.sh` script is the preferred way to configure tiered locality because the same
script can be used for Alluxio servers and compute applications. However, in some situations
you might not have access to applications' classpaths, so the `alluxio-locality.sh` approach is
not possible. You can still configure tiered locality by setting `alluxio.locality.[tiername]`
configuration properties, e.g.

```
alluxio.locality.node=node_name
alluxio.locality.rack=rack_name
```

See the [Configuration-Settings]({{ site.baseurl }}{% link en/basic/Configuration-Settings.md %}) page for the different
ways to set configuration properties.

### Custom locality script name

By default Alluxio clients and servers search the classpath for a script named `alluxio-locality.sh`. You can override this name by setting

```
alluxio.locality.script=locality_script_name
```

### Node locality priority order

There are multiple ways to configure node locality. This is the order of precedence,
from highest priority to lowest priority:

1. Set `alluxio.locality.node`
1. Set `node=...` in the output of the script configured by `alluxio.locality.script`
1. Set `alluxio.worker.hostname` on a worker, `alluxio.master.hostname` on a master, or
`alluxio.user.hostname` on a client.
1. If none of the above are configured, node locality is determined by localhost lookup.

### When exactly is tiered locality used?

1. When clients choose which worker to read through during UFS reads.
1. When clients choose which worker to read from when multiple Alluxio workers hold a block.
1. If using the `LocalFirstPolicy` or `LocalFirstAvoidEvictionPolicy`, clients use tiered locality
to choose which worker to write to when writing data to Alluxio.

