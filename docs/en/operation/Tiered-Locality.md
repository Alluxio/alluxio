---
layout: global
title: Tiered Locality
nickname: Tiered Locality
group: Operations
priority: 13
---

* Table of Contents
{:toc}

The tiered locality feature enables applications to make intelligent, topology-based decisions
regarding which Alluxio workers to read from and write to.

## Motivation

For an application running on `host1`, reading data from an Alluxio worker also on `host1` is typically more
efficient than reading from a different worker on `host2`.
Similarly, reading from a worker on the same rack, or in the same availability zone,
or in the same data center is faster than reading
from a worker on a different rack, different availability zone, or different data center.
Tiered locality allows users to take advantage of various locality levels by configuring
servers and clients with network topology information.

## Tiered Identity

To indicate different locality levels, one can assign an Alluxio
master, worker, or client with a *Tiered Identity*.
This identity is an address tuple in the format **`(tierName1=value1, tierName2=value2, ...)`**
where each entry **`tierName1=value1`** is called a *locality tier*.
Alluxio clients will favor interacting with workers that share identical identity entries in the
provided order.

### Example

For example, a client with an identity **`(node=A, rack=rack1, datacenter=dc1)`** will prefer to
read from or write to a worker at **`(node=B, rack=rack1, datacenter=dc1)`** over a worker at
**`(node=C, rack=rack2, datacenter=dc1)`** because:
- no worker shares the same `node` value as the client
- the first worker shares the same `rack` value, `rack1`, as the client
- the `datacenter` entry is ignored because at least one match was found in the previous locality
tier

### When is tiered locality used?

1. When clients choose which worker to read through during UFS reads
1. When clients choose which worker to read from when multiple Alluxio workers hold a block
1. If using the `LocalFirstPolicy` or `LocalFirstAvoidEvictionPolicy`, clients use tiered locality
to choose which worker to write to when writing data to Alluxio

## Basic Setup

Alluxio configures two locality tiers by default: `node` and `rack`.
One can set the tier value for a worker on `host1` of `rack2` in `alluxio-site.properties`:

```
alluxio.locality.node=host1
alluxio.locality.rack=rack2
```

These configurations will assign this worker a tiered identity of `(node=host1, rack=rack2)`.

To verify that the configuration is working, check the
[master, worker, and application logs]({{ '/en/operation/Basic-Logging.html' | relativize_url }}#log-location).
A log entry should appear of the format:

```log
INFO  TieredIdentityFactory - Initialized tiered identity TieredIdentity(node=host1, rack=rack2)
```

If the log entry does not appear, try running the locality script directly to check its output and
ensure it is executable by the user that launched the Alluxio server.

## Advanced

### Custom locality tiers

Users may customize locality tiers to take advantage of more advanced topologies.
The list of tiers is determined by the
[`alluxio.locality.order`]({{ '/en/operation/Configuration.html' | relativize_url }}#alluxio.locality.order) property on the master.
The order should go from most to least specific.
For example, to add availability zone locality to a cluster, set:

```
alluxio.locality.order=node,rack,availability_zone
```

If the user does nothing to provide tiered identity info,
each entity will perform a hostname lookup to set its node-level identity info.
If other locality tiers are left unset, they will not be used to inform locality decisions.

### Setting locality

#### Option A: shell script

By default, Alluxio clients and servers search the classpath for a script named `alluxio-locality.sh`.
The output format of this script is a comma-separated list of `tierName=tierValue` pairs.

For example, suppose Alluxio workers are spread across multiple availability zones within EC2.
To configure tiered locality with availability zones:

1. On the Alluxio master(s), set `alluxio.locality.order=node,availability_zone` to define the order
   of locality tiers to prefer `node` over `availability_zone`.

1. Create a script `alluxio-locality.sh` to determine the tiered identity for each entity
   (the name of this script can be overridden by setting `alluxio.locality.script`).
   The output format is a comma-separated list of `tierName=tierValue` pairs.
   ```bash
   #!/usr/bin/env bash

   node=$(hostname)
   # Ask EC2 which availability zone we're in
   availability_zone=$(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone)

   echo "node=${node},availability_zone=${availability_zone}"
   ```

1. Make the script executable with `chmod +x alluxio-locality.sh`.

1. Include the script on the classpath of your applications and Alluxio servers.
   For servers, put the file in the `${ALLUXIO_HOME}/conf/` directory.

1. Restart Alluxio servers to pick up the configuration changes.

A log entry should appear of the format:

```log
INFO  TieredIdentityFactory - Initialized tiered identity TieredIdentity(node=ip-xx-xx-xx-xx, availability_zone=us-east-1)
```

#### Option B: configuration properties

Using the locality shell script is the preferred way to configure tiered locality over configuration
properties because the same script can be used for Alluxio servers and compute applications.
In situations where users do not have access to application classpaths,
tiered locality can be configured by setting `alluxio.locality.[tiername]`:

```
alluxio.locality.node=node_name
alluxio.locality.rack=rack_name
```

See the [Configuration-Settings]({{ '/en/operation/Configuration.html' | relativize_url }})
page for the different ways to set configuration properties.

### Custom locality tier order

For every tier name (e.g. `node`, `rack`, `availibility_zone` etc.) the order of precedence for
obtaining its value, from highest priority to lowest priority, is as follows:

1. From `alluxio.locality.node` set in `alluxio-site.properties`

1. From `node=...` in the output of the script, whose name is configured by
`alluxio.locality.script`

In order to supply a default value for a particular `node` tier, the above list is followed by two more
sources, from highest to lowest priority:

1. From `alluxio.worker.hostname` on a worker, `alluxio.master.hostname` on a master, or
`alluxio.user.hostname` on a client in their respective `alluxio-site.properties`

1. If none of the above configurations are set, node locality is determined by hostname lookup
