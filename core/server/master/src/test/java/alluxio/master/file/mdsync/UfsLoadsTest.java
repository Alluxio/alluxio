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

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;

import alluxio.AlluxioURI;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsClient;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UfsLoadsTest {
  ExecutorService mThreadPool;
  TaskTracker mTaskTracker;
  MetadataSyncHandler mMetadataSyncHandler;
  MockUfsClient mUfsClient;
  UfsSyncPathCache mUfsSyncPathCache;
  UfsAbsentPathCache mAbsentPathCache;
  SyncProcess mSyncProcess;
  List<UfsStatus> mProcessedItems;
  UfsStatus mFileStatus = new UfsFileStatus("file", "",
      0L, 0L, "", "", (short) 0, 0L);
  UfsStatus mDirStatus = new UfsDirectoryStatus("dir", "", "", (short) 0);
  static final long WAIT_TIMEOUT = 5_000;

  private CloseableResource<UfsClient> getClient(AlluxioURI ignored) {
    return new CloseableResource<UfsClient>(mUfsClient) {
      @Override
      public void closeResource() {
      }
    };
  }

  @Before
  public void before() throws Throwable {
    mThreadPool = Executors.newCachedThreadPool();
    mUfsClient = Mockito.spy(new MockUfsClient());
    mSyncProcess = Mockito.spy(new DummySyncProcess());
    mProcessedItems = new ArrayList<>();
    Mockito.doAnswer(ans -> {
      LoadResult result = ans.getArgument(0);
      result.getUfsLoadResult().getItems().peek(mProcessedItems::add);
      return ans.callRealMethod();
    }).when(mSyncProcess).performSync(any(LoadResult.class), any(UfsSyncPathCache.class));
    mAbsentPathCache = Mockito.mock(UfsAbsentPathCache.class);
    mUfsSyncPathCache = Mockito.mock(UfsSyncPathCache.class);
    mTaskTracker = new TaskTracker(
        1, 1, false, false,
        mUfsSyncPathCache, mAbsentPathCache, mSyncProcess, this::getClient);
    mMetadataSyncHandler = new MetadataSyncHandler(mTaskTracker, null, null);
  }

  @After
  public void after() throws Throwable {
    assertFalse(mTaskTracker.hasRunningTasks());
    mTaskTracker.close();
    mThreadPool.shutdown();
  }

  @Test
  public void singleFileSync() {
    mUfsClient.setGetStatusItem(mFileStatus);
  }
}
