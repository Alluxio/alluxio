---
layout: global
title: Tiered Locality
nickname: Tiered Locality
group: Features
priority: 1
---

* Table of Contents
{:toc}

# Tiered Identity

In Alluxio, each entity (masters, workers, clients) has a *Tiered Identity*. A tiered
identity is an address tuple such as `(node=..., rack=...)`. Each pair in the tuple is called
a *locality tier*. The locality tiers are ordered from most specific to least specific, so if
two entities have the same node name, they are assumed to also have the same rack name. Alluxio
uses tiered identities to optimize for locality. For example, when the client wants to read a
file from the UFS, it will prefer to read through an Alluxio worker on the same node.
If there is no local worker in the first tier (node), the next tier (rack) will be checked
to prefer rack-local data transfer. If no workers share a node or rack with the client, an
arbitrary worker will be chosen.

# Configuration

If the user does nothing to provide tiered identity info, each entity will
perform a localhost lookup to set its node-level identity info. If other locality tiers
are left unset, they will not be used to inform locality decisions. To set
the value for a locality tier, set the configuration property

```
alluxio.locality.[tiername]=...
```

See the [Configuration-Settings](Configuration-Settings.html) page for details on how
to set configuration properties.

It is also possible to configure tiered identity info via script. By default Alluxio searches
the classpath for a script named `alluxio-locality.sh`. You can override this name by setting

```
alluxio.locality.script=/path/to/script
```

The script should be executable and output a comma-separated list of `tierName=...`
pairs. Here is an example script for reference:

```bash
#!/bin/bash

echo "host=$(hostname),rack=/rack1"
```

If the no script exists at `alluxio.locality.script`, the property will be ignored. If
the script returns a nonzero exit code or returns malformed output, an error will be
raised in Alluxio.

## Node locality priority order

There are multiple ways to configure node locality. This is the order of precedence,
from highest priority to lowest priority:

1. Set `alluxio.locality.node`
1. Set `node=...` in the output of the script configured by `alluxio.locality.script`
1. Set `alluxio.worker.hostname` on a worker, `alluxio.master.hostname` on a master, or
`alluxio.user.hostname` on a client.
1. If none of the above are configured, node locality is determined by localhost lookup.

# When is tiered locality used?

1. When choosing a worker to read through during UFS reads.
1. When choosing a worker to read from when multiple Alluxio workers hold a block.
1. If using the `LocalFirstPolicy` or `LocalFirstAvoidEvictionPolicy`, tiered locality is
used to choose which worker to write to when writing data to Alluxio.

# Custom locality tiers

By default, Alluxio has two locality tiers: `node` and `rack`. Users may customize the
set of locality tiers to take advantage of more advanced topologies. To change the set
of tiers available, set `alluxio.locality.order`. The order should go from most specific
to least specific. For example, to add availability zone locality to a cluster, set

```
alluxio.locality.order=node,rack,availability_zone
```

Note that this configuration must be set for all entities, including Alluxio clients.

Now to set the availability zone for each entity, either set the
`alluxio.locality.availability_zone` property key, or use a locality script and include
`availability_zone=...` in the output.
