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

package alluxio.master.file.metasync;

import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.master.metastore.ReadOnlyInodeStore;

import java.util.concurrent.Semaphore;

/**
 * The metadata syncer.
 */
// TODO move this to the test package
public class TestMetadataSyncer extends MetadataSyncer {
  @FunctionalInterface
  public interface Callback {
    void apply() throws Exception;
  }

  public TestMetadataSyncer(DefaultFileSystemMaster fsMaster, ReadOnlyInodeStore inodeStore,
                            MountTable mountTable, InodeTree inodeTree,
                            UfsSyncPathCache syncPathCache) {
    super(fsMaster, inodeStore, mountTable, inodeTree, syncPathCache);
  }

  Semaphore mLock = new Semaphore(0);
  private int mBlockOnNth = -1;
  private int mSyncCount = 0;
  private Callback mCallback = null;

  /**
   * Blocks the current thread until the nth inode sync (root included) is ABOUT TO execute,
   * executes the callback and resumes the sync.
   * Used for testing concurrent modifications.
   * @param nth the inode sync count
   * @param callback the callback to execute
   */
  public synchronized void blockUntilNthSyncThenDo(int nth, Callback callback)
      throws InterruptedException {
    mBlockOnNth = nth;
    mCallback = callback;
    mLock.acquire();
  }
}
