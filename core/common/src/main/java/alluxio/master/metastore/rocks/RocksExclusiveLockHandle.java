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

import alluxio.exception.runtime.AlluxioRuntimeException;

import java.util.concurrent.Callable;

/**
 * This is a handle used to manage the write lock(exclusive lock) on RocksStore.
 * The exclusive lock is acquired when ref count is zero, and the StopServing flag ensures
 * no new r/w will come in, so the ref count will stay zero throughout the period.
 *
 * One exception is when the exclusive lock is forced (ignoring uncompleted r/w operations),
 * when the reader comes back the exclusive lock is already held. At this moment when the late
 * reader comes back, it should not update the ref count anymore. See Javadoc on
 * {@link RocksSharedLockHandle#close()} for how that is handled.
 */
public class RocksExclusiveLockHandle implements AutoCloseable {
  private final Callable<Void> mCloseAction;

  /**
   * The constructor.
   * @param closeAction the action called on close
   */
  public RocksExclusiveLockHandle(Callable<Void> closeAction) {
    mCloseAction = closeAction;
  }

  @Override
  public void close() {
    try {
      mCloseAction.call();
    } catch (Exception e) {
      // From the current usage in RocksStore, this is unreachable
      throw AlluxioRuntimeException.from(e);
    }
  }
}
