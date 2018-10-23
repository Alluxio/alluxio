---
layout: global
title: Loading Under File Storage Metadata
nickname: Loading UFS Metadata
group: Features
priority: 0
---

* Table of Contents
{:toc}

Alluxio provides a unified namespace, acting like a cache for data in one or more
under file storage (UFS) systems. This page discusses how Alluxio interacts with
UFSes to discover UFS files and make them available through Alluxio.

Alluxio aims to be transparent. That is, accessing UFS files through Alluxio
should behave the same as accessing them directly through the UFS. For example,
if the root UFS is `s3://bucket/data`, listing `alluxio:///` should show the
same result as listing `s3://bucket/data`, and running `cat` on `alluxio:///file`
should show the same result as running `cat` on `s3://bucket/data/file`.

To efficiently achieve this transparency, Alluxio lazily loads metadata from the
UFS. In the above example, Alluxio will not know about `s3://bucket/data/file` at
startup. `file` will only be discovered when the user lists `alluxio:///` or tries
to cat `alluxio:///file`. This makes mounting a new UFS very cheap, and prevents
unnecessary work.

By default, *Alluxio expects that all modifications to UFSes happen through Alluxio*.
This allows Alluxio to only scan each UFS directory a single time, significantly
improving performance when UFS metadata operations are slow. As long as all UFS
changes go through Alluxio, this has no user-facing impact.
However, it is sometimes necessary for Alluxio to handle out of band UFS changes.
That is where the metadata sync feature comes into play.

## UFS Metadata Sync

> The UFS metadata sync feature has been available since version `1.7.0`.

When Alluxio scans a UFS directory and loads metadata for its sub-paths, it
creates a cached copy of the metadata so that future operations don't need to
hit the UFS. By default the cached copy lives forever, but you can configure the
cache timeout with the `alluxio.user.file.metadata.sync.interval` configuration
property. This is a client-side property, so when a client makes a request, it
can specify `alluxio.user.file.metadata.sync.interval=1m` to say "when running
this operation, make sure all relevant metadata has been synced within the last
1 minute". Setting a value of `0` means that metadata will always be synced. The
default value of `-1` means to never re-sync metadata.

Low values for `alluxio.user.file.metadata.sync.interval` allow Alluxio clients
to quickly observe out of band UFS changes, but increase the number of calls to
the UFS, decreasing RPC response time performance.

Metadata sync keeps a fingerprint of each UFS file so that Alluxio can re-sync
the file if it changes. The fingerprint includes information such as file size
and last modified time. If a file is modified in the UFS, Alluxio
will detect this from the fingerprint, free the existing data for that file, and
re-load the metadata for the updated file. If a file is deleted in the UFS, Alluxio
will delete the file from Alluxio's namespace as well. Lastly, Alluxio will detect
newly added files and make them available to Alluxio clients.

## Techniques for managing UFS sync

### Daily Metadata Sync

If your UFS is only updated once a day, you can run

`alluxio fs ls -R -Dalluxio.user.file.metadata.sync.interval=0 /`

after the update, then use the default `-1` for the rest of the day to avoid calls
to the UFS.

### Centralized Configuration

For clusters where the UFS is often changing and jobs needs to see the updates,
it can be inconvenient for every client to need to specify a sync interval. To
address this, set a default sync interval on the master, and all requests will
use that sync interval by default.

In masters' `alluxio-site.properties`:

`alluxio.user.file.metadata.sync.interval=1m`

Note that master needs to be restarted to pick up configuration changes.

## Other Methods for Loading New UFS Files

The UFS sync discussed previously is the recommended method for loading files from
the UFS. There are a couple other methods mentioned here for completeness.

`alluxio.user.file.metadata.load.type`: This property can be set to either
`ALWAYS`, `ONCE`, or `NEVER`. It acts similar to
`alluxio.user.file.metadata.sync.interval`, but with two caveats: (1) It only
discovers new files, and doesn't re-load UFS-modified files or remove UFS-deleted
files, and (2) it only applies to the `exists`, `list`, and `getStatus` RPCs.
`ALWAYS` will always check the UFS for new files, `ONCE` will use the default
behavior of only scanning each directory once ever, and `NEVER` will prevent Alluxio
from scanning for new files at all.

`alluxio fs ls -f /path`: The `-f` option to `ls` acts the same as setting
`alluxio.user.file.metadata.load.type` to `ALWAYS`. It discovers new files, but
doesn't detect modified or deleted UFS files. For that, instead pass the
`-Dalluxio.user.file.metadata.sync.interval=0` option to `ls`.
