package alluxio.master.mdsync;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import alluxio.AlluxioURI;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsClient;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class UfsLoadsTest {

  ExecutorService mThreadPool;
  TaskTracker mTaskTracker;
  MdSync mMdSync;
  MockUfsClient mUfsClient;
  UfsSyncPathCache mUfsSyncPathCache;
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
    mSyncProcess = Mockito.spy(new TestSyncProcess());
    mProcessedItems = new ArrayList<>();
    Mockito.doAnswer(ans -> {
      LoadResult result = ans.getArgument(0);
      result.getUfsLoadResult().getItems().peek(mProcessedItems::add);
      return ans.callRealMethod();
    }).when(mSyncProcess).performSync(any(LoadResult.class), any(UfsSyncPathCache.class));

    mUfsSyncPathCache = Mockito.mock(UfsSyncPathCache.class);
    mTaskTracker = new TaskTracker(
        1, 1, false, false,
        mUfsSyncPathCache, mSyncProcess, this::getClient);
    mMdSync = new MdSync(mTaskTracker);
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
