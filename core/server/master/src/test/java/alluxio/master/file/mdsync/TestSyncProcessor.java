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

package alluxio.master.file.mdsync;

import alluxio.exception.AccessControlException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.meta.InodeIterationResult;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.master.metastore.ReadOnlyInodeStore;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import javax.annotation.Nullable;

/**
 * The metadata syncer.
 */
public class TestSyncProcessor extends DefaultSyncProcess {
  @FunctionalInterface
  public interface Callback {
    void apply() throws Exception;
  }

  @FunctionalInterface
  public interface SyncOneCallback {
    void apply(SyncProcessContext context) throws Exception;
  }

  public TestSyncProcessor(DefaultFileSystemMaster fsMaster, ReadOnlyInodeStore inodeStore,
                           MountTable mountTable, InodeTree inodeTree,
                           UfsSyncPathCache syncPathCache, UfsAbsentPathCache absentPathCache) {
    super(fsMaster, inodeStore, mountTable, inodeTree, syncPathCache, absentPathCache);
  }

  Semaphore mLock = new Semaphore(0);
  private int mBlockOnNth = -1;
  private int mSyncCount = 0;
  private Callback mCallback = null;
  private SyncOneCallback mCallbackBeforePerformSyncOne = null;

  @Override
  protected SingleInodeSyncResult performSyncOne(SyncProcessState syncState,
                                                 @Nullable UfsItem currentUfsStatus,
                                                 @Nullable InodeIterationResult currentInode)
      throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistsException,
      IOException, BlockInfoException, DirectoryNotEmptyException, AccessControlException {
    if (mCallbackBeforePerformSyncOne != null) {
      try {
        mCallbackBeforePerformSyncOne.apply(syncState.mContext);
      } catch (Exception e) {
        throw new RuntimeException();
      }
    }
    mSyncCount++;
    if (mSyncCount == mBlockOnNth && mCallback != null) {
      try {
        mCallback.apply();
      } catch (Exception e) {
        throw new RuntimeException();
      }
      mLock.release();
    }
    return super.performSyncOne(syncState, currentUfsStatus, currentInode);
  }

  public synchronized void beforePerformSyncOne(SyncOneCallback callback)
      throws InterruptedException {
    mCallbackBeforePerformSyncOne = callback;
  }

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
