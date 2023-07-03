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

import java.util.concurrent.atomic.LongAdder;

/**
 * This is a handle used to manage a read lock(shared lock) on RocksStore.
 * When the shared lock is held, exclusive locks will wait. That guarantees the RocksDB
 * is not wiped out/closed while an r/w operation is active.
 *
 * RocksStore uses ref count for locking so releasing a read lock is just decrementing the
 * reference count.
 */
public class RocksSharedLockHandle implements AutoCloseable {
  private final int mDbVersion;
  private final LongAdder mRefCount;

  /**
   * The constructor.
   *
   * @param dbVersion The RocksDB version. This version is updated when the RocksDB
   *                  is restored or wiped out.
   * @param refCount the ref count to decrement on close
   */
  public RocksSharedLockHandle(int dbVersion, LongAdder refCount) {
    mDbVersion = dbVersion;
    mRefCount = refCount;
  }

  /**
   * Gets the version on the lock.
   * @return version
   */
  public int getLockVersion() {
    return mDbVersion;
  }

  @Override
  public void close() {
    /*
     * If the exclusive lock has been forced and the ref count is reset, this reference will point
     * to an out-of-date counter. Therefore, we can just update this counter without concerns.
     * If the exclusive lock is has NOT been forced, we decrement the ref count normally.
     * If the exclusive lock has been forced, we decrement an irrelevant counter which will never
     * be read.
     */
    mRefCount.decrement();
  }
}
