/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.metastore.rocks;

import javax.annotation.concurrent.ThreadSafe;

/**
 * An object wrapper for RocksDB status. Two states are included.
 * The StopServing flag is an indicator that RocksDB will stop serving shortly.
 * This can be because the RocksDB will be closed, rewritten or wiped out.
 * This StopServing flag is used in:
 * 1. The shared lock will check this flag and give up the access early
 * 2. An ongoing r/w (e.g. an iterator) will check this flag during iteration
 *    and abort the iteration. So it will not block the RocksDB from shutting down.
 *
 * The version is needed because RocksBlockMetaStore and RocksInodeStore may clear and restart
 * the RocksDB. If the data in RocksDB has changed, the version will change. For example:
 * 1. If the RocksDB is cleared or restored to a checkpoint, the version will increment.
 * 2. If the RocksDB is locked because the master will dump a checkpoint, the version will
 *    not increment when the checkpoint operation is complete and the lock is released.
 *
 * An instance of this class is immutable. So seeing the same instance indicates the state has
 * not changed. Whenever the state is changed, create a new instance.
 */
@ThreadSafe
public class VersionedRocksStoreStatus {
  public final boolean mStopServing;
  public final int mVersion;

  public VersionedRocksStoreStatus(boolean closed, int version) {
    mStopServing = closed;
    mVersion = version;
  }
}
