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

package alluxio.master.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.file.options.DescendantType;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockingScheme;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.NoopUfsAbsentPathCache;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.master.journal.JournalType;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.user.UserState;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UfsStatusCache;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.local.LocalUnderFileSystem;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import com.codahale.metrics.Counter;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UnderFileSystem.Factory.class})
public class FileSystemMasterSyncMetadataMetricsTest {
  private static final AlluxioURI ROOT = new AlluxioURI("/");
  private static final String TEST_DIR_PREFIX = "/dir";
  private static final String TEST_FILE_PREFIX = "/file";
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  private String mUfsUri;
  private ExceptionThrowingLocalUnderFileSystem mUfs;
  private ExecutorService mExecutorService;
  private MasterRegistry mRegistry;
  private DefaultFileSystemMaster mFileSystemMaster;

  @Before
  public void before() throws Exception {
    UserState s = UserState.Factory.create(Configuration.global());
    AuthenticatedClientUser.set(s.getUser().getName());

    mTempDir.create();
    mUfsUri = mTempDir.newFolder().getAbsolutePath();

    mUfs = new ExceptionThrowingLocalUnderFileSystem(new AlluxioURI(mUfsUri),
        UnderFileSystemConfiguration.defaults(Configuration.global()));
    PowerMockito.mockStatic(UnderFileSystem.Factory.class);
    Mockito.when(UnderFileSystem.Factory.create(anyString(), any())).thenReturn(mUfs);

    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
    Configuration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mUfsUri);
    String journalFolderUri = mTempDir.newFolder().getAbsolutePath();

    mExecutorService = Executors
        .newFixedThreadPool(4, ThreadFactoryUtils
            .build("FileSystemMasterSyncMetadataMetricsTest-%d", true));
    mRegistry = new MasterRegistry();
    JournalSystem journalSystem =
        JournalTestUtils.createJournalSystem(journalFolderUri);
    CoreMasterContext context = MasterTestUtils.testMasterContext(journalSystem);
    new MetricsMasterFactory().create(mRegistry, context);
    BlockMaster blockMaster = new BlockMasterFactory().create(mRegistry, context);
    mFileSystemMaster = new DefaultFileSystemMaster(blockMaster, context,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mRegistry.add(FileSystemMaster.class, mFileSystemMaster);
    journalSystem.start();
    journalSystem.gainPrimacy();
    mRegistry.start(true);

    MetricsSystem.resetAllMetrics();
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
    mExecutorService.shutdown();
  }

  @Test
  public void metadataSyncMetrics() throws Exception {
    final Counter streamCount = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_COUNT;
    final Counter succeededStreams = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SUCCESS;
    final Counter failedStreams = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_FAIL;
    final Counter noChangedPaths = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_NO_CHANGE;
    final Counter skippedStreams = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SKIPPED;
    final Counter succeededPaths =
        DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SYNC_PATHS_SUCCESS;
    final Counter failedPaths = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SYNC_PATHS_FAIL;
    final Counter totalTimeMs = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_TIME_MS;

    int previousStreamCount = 0;
    int previousSucceededStreams = 0;
    int previousFailedStreams = 0;
    int previousNoChangedPaths = 0;
    int previousSkippedStreams = 0;
    int previousSucceededPaths = 0;
    int previousFailedPaths = 0;

    final int threadNum = 10;
    final int fileNum = 20;

    //create test files in UFS
    for (int i = 0; i < threadNum; i++) {
      String dir = TEST_DIR_PREFIX + i;
      createUfsDir(dir);
      for (int j = 0; j < fileNum; j++) {
        createUfsFile(dir + TEST_FILE_PREFIX + j).close();
      }
    }

    // Force sync the root path
    // The sync should succeed
    LockingScheme syncScheme =
        new LockingScheme(ROOT, InodeTree.LockPattern.READ, true);
    InodeSyncStream syncStream =
        new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
            DescendantType.ALL, ListStatusContext.defaults().getOptions().getCommonOptions(),
            false, true, false, false);
    syncStream.sync();

    assertEquals(previousStreamCount + 1, streamCount.getCount());
    previousStreamCount += 1;
    assertEquals(previousSucceededStreams + 1, succeededStreams.getCount());
    previousSucceededStreams += 1;
    // "/" , "/dir*" and "/dir*/file*"
    assertEquals(previousSucceededPaths + (1 + threadNum * (1 + fileNum)),
        succeededPaths.getCount());
    previousSucceededPaths += (1 + threadNum * (1 + fileNum));
    assertEquals(previousFailedStreams + 0, failedStreams.getCount());
    previousFailedStreams += 0;
    assertEquals(previousFailedPaths + 0, failedPaths.getCount());
    previousFailedPaths += 0;
    // "/" , "/dir*" and "/dir*/file*"
    assertEquals(previousNoChangedPaths + (1 + threadNum * (1 + fileNum)),
        noChangedPaths.getCount());
    previousNoChangedPaths += (1 + threadNum * (1 + fileNum));
    assertEquals(previousSkippedStreams + 0, skippedStreams.getCount());
    previousSkippedStreams += 0;

    String path = TEST_DIR_PREFIX + "0" + TEST_FILE_PREFIX + "0";
    //Overwrite the path(/dir0/file0) in UFS
    //The sync should succeed and pick up the change
    OutputStream outputStream = createUfsFile(path);
    outputStream.write(new byte[] {0, 1});
    outputStream.close();
    syncScheme = new LockingScheme(new AlluxioURI(path), InodeTree.LockPattern.READ, true);
    syncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.NONE, ListStatusContext.defaults().getOptions().getCommonOptions(),
        false, true, false, false);
    syncStream.sync();

    assertEquals(previousStreamCount + 1, streamCount.getCount());
    previousStreamCount += 1;
    assertEquals(previousSucceededStreams + 1, succeededStreams.getCount());
    previousSucceededStreams += 1;
    assertEquals(previousSucceededPaths + 1, succeededPaths.getCount());
    previousSucceededPaths += 1;
    assertEquals(previousFailedStreams + 0, failedStreams.getCount());
    previousFailedStreams += 0;
    assertEquals(previousFailedPaths + 0, failedPaths.getCount());
    previousFailedPaths += 0;
    assertEquals(previousNoChangedPaths + 0, noChangedPaths.getCount());
    previousNoChangedPaths += 0;
    assertEquals(previousSkippedStreams + 0, skippedStreams.getCount());
    previousSkippedStreams += 0;

    //Force sync the path(/dir0/file0) again
    //The sync should succeed with no change
    syncScheme = new LockingScheme(new AlluxioURI(path), InodeTree.LockPattern.READ, true);
    syncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.NONE, ListStatusContext.defaults().getOptions().getCommonOptions(),
        false, true, false, false);
    syncStream.sync();

    assertEquals(previousStreamCount + 1, streamCount.getCount());
    previousStreamCount += 1;
    assertEquals(previousSucceededStreams + 1, succeededStreams.getCount());
    previousSucceededStreams += 1;
    assertEquals(previousSucceededPaths + 1, succeededPaths.getCount());
    previousSucceededPaths += 1;
    assertEquals(previousFailedStreams + 0, failedStreams.getCount());
    previousFailedStreams += 0;
    assertEquals(previousFailedPaths + 0, failedPaths.getCount());
    previousFailedPaths += 0;
    assertEquals(previousNoChangedPaths + 1, noChangedPaths.getCount());
    previousNoChangedPaths += 1;
    assertEquals(previousSkippedStreams + 0, skippedStreams.getCount());
    previousSkippedStreams += 0;

    //Sync the path(/dir0/file0) again, the attempt is not forced so should be skipped
    syncScheme = new LockingScheme(new AlluxioURI(path), InodeTree.LockPattern.READ, false);
    syncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.NONE, ListStatusContext.defaults().getOptions().getCommonOptions(),
        false, false, false, false);
    syncStream.sync();

    assertEquals(previousStreamCount + 1, streamCount.getCount());
    previousStreamCount += 1;
    assertEquals(previousSucceededStreams + 0, succeededStreams.getCount());
    previousSucceededStreams += 0;
    assertEquals(previousSucceededPaths + 0, succeededPaths.getCount());
    previousSucceededPaths += 0;
    assertEquals(previousFailedStreams + 0, failedStreams.getCount());
    previousFailedStreams += 0;
    assertEquals(previousFailedPaths + 0, failedPaths.getCount());
    previousFailedPaths += 0;
    assertEquals(previousNoChangedPaths + 0, noChangedPaths.getCount());
    previousNoChangedPaths += 0;
    assertEquals(previousSkippedStreams + 1, skippedStreams.getCount());
    previousSkippedStreams += 1;

    // The sync should succeed and the paths(/dir0/file0) should be removed from inodeTree
    // because inodeTree contains the paths when ufs throwing IOException
    mUfs.mIsThrowIOException = true;
    syncScheme = new LockingScheme(new AlluxioURI(path), InodeTree.LockPattern.READ, true);
    syncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.NONE, ListStatusContext.defaults().getOptions().getCommonOptions(),
        false, true, false, false);
    syncStream.sync();

    assertEquals(previousStreamCount + 1, streamCount.getCount());
    previousStreamCount += 1;
    assertEquals(previousSucceededStreams + 1, succeededStreams.getCount());
    previousSucceededStreams += 1;
    assertEquals(previousSucceededPaths + 1, succeededPaths.getCount());
    previousSucceededPaths += 1;
    assertEquals(previousFailedStreams + 0, failedStreams.getCount());
    previousFailedStreams += 0;
    assertEquals(previousFailedPaths + 0, failedPaths.getCount());
    previousFailedPaths += 0;
    assertEquals(previousNoChangedPaths + 0, noChangedPaths.getCount());
    previousNoChangedPaths += 0;
    assertEquals(previousSkippedStreams + 0, skippedStreams.getCount());
    previousSkippedStreams += 0;

    // Now the path(/dir0/file0) is non-existent in inodeTree and existent in UFS
    // The sync should fail because UFS throws IOException
    mUfs.mIsThrowIOException = true;
    syncScheme = new LockingScheme(new AlluxioURI(path), InodeTree.LockPattern.READ, true);
    syncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.NONE, ListStatusContext.defaults().getOptions().getCommonOptions(),
        false, true, false, false);
    syncStream.sync();

    assertEquals(previousStreamCount + 1, streamCount.getCount());
    previousStreamCount += 1;
    assertEquals(previousSucceededStreams + 0, succeededStreams.getCount());
    previousSucceededStreams += 0;
    assertEquals(previousSucceededPaths + 0, succeededPaths.getCount());
    previousSucceededPaths += 0;
    assertEquals(previousFailedStreams + 1, failedStreams.getCount());
    previousFailedStreams += 1;
    assertEquals(previousFailedPaths + 1, failedPaths.getCount());
    previousFailedPaths += 1;
    assertEquals(previousNoChangedPaths + 0, noChangedPaths.getCount());
    previousNoChangedPaths += 0;
    assertEquals(previousSkippedStreams + 0, skippedStreams.getCount());
    previousSkippedStreams += 0;

    //The path(/dir/file) are non-existent in inodeTree and UFS
    //The sync should fail
    mUfs.mIsThrowIOException = false;
    syncScheme = new LockingScheme(new AlluxioURI(TEST_DIR_PREFIX + threadNum + TEST_FILE_PREFIX),
        InodeTree.LockPattern.READ, true);
    syncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.NONE, ListStatusContext.defaults().getOptions().getCommonOptions(),
        false, true, false, false);
    syncStream.sync();

    assertEquals(previousStreamCount + 1, streamCount.getCount());
    previousStreamCount += 1;
    assertEquals(previousSucceededStreams + 0, succeededStreams.getCount());
    previousSucceededStreams += 0;
    assertEquals(previousSucceededPaths + 0, succeededPaths.getCount());
    previousSucceededPaths += 0;
    assertEquals(previousFailedStreams + 1, failedStreams.getCount());
    previousFailedStreams += 1;
    assertEquals(previousFailedPaths + 1, failedPaths.getCount());
    previousFailedPaths += 1;
    assertEquals(previousNoChangedPaths + 0, noChangedPaths.getCount());
    previousNoChangedPaths += 0;
    assertEquals(previousSkippedStreams + 0, skippedStreams.getCount());
    previousSkippedStreams += 0;

    class TestThread extends Thread {
      public InodeSyncStream mInodeSyncStream;
      public CountDownLatch mLatch;

      @Override
      public void run() {
        try {
          mInodeSyncStream.sync();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        mLatch.countDown();
      }
    }
    //Test in multiple threads
    //Each thread sync its own folder(/dir*)

    TestThread[] testThreads = new TestThread[threadNum];
    CountDownLatch latch = new CountDownLatch(threadNum);
    for (int i = 0; i < threadNum; i++) {
      testThreads[i] = new TestThread();
      LockingScheme lockingScheme = new LockingScheme(new AlluxioURI(TEST_DIR_PREFIX + i),
          InodeTree.LockPattern.READ, true);
      testThreads[i].mInodeSyncStream =
          new InodeSyncStream(lockingScheme, mFileSystemMaster, RpcContext.NOOP,
              DescendantType.ALL, ListStatusContext.defaults().getOptions().getCommonOptions(),
              false, true, false, false);
      testThreads[i].mLatch = latch;
    }
    for (Thread t : testThreads) {
      t.start();
    }
    latch.await();

    assertEquals(previousStreamCount + threadNum, streamCount.getCount());
    previousStreamCount += threadNum;
    assertEquals(previousSucceededStreams + threadNum, succeededStreams.getCount());
    previousSucceededStreams += threadNum;
    assertEquals(previousSucceededPaths + threadNum * (1 + fileNum), succeededPaths.getCount());
    previousSucceededPaths += threadNum * (1 + fileNum);
    assertEquals(previousFailedStreams + 0, failedStreams.getCount());
    previousFailedStreams += 0;
    assertEquals(previousFailedPaths + 0, failedPaths.getCount());
    previousFailedPaths += 0;
    assertEquals(previousNoChangedPaths + threadNum * (1 + fileNum), noChangedPaths.getCount());
    previousNoChangedPaths += threadNum * (1 + fileNum);
    assertEquals(previousSkippedStreams + 0, skippedStreams.getCount());
    previousSkippedStreams += 0;
    assertTrue(totalTimeMs.getCount() > 0);
  }

  @Test
  public void metadataPrefetchMetrics() throws Exception {
    final Counter prefetchOpsCount =
        DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_OPS_COUNT;
    final Counter succeededPrefetches =
        DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_SUCCESS;
    final Counter failedPrefetches = DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_FAIL;
    final Counter canceledPrefetches =
        DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_CANCEL;
    final Counter prefetchPaths = DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_PATHS;
    final Counter prefetchRetries = DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_RETRIES;

    int previousPrefetchOpsCount = 0;
    int previousSucceededPrefetches = 0;
    int previousFailedPrefetches = 0;
    int previousCancelPrefetches = 0;
    int previousPrefetchPaths = 0;

    UfsStatusCache ufsStatusCache = new UfsStatusCache(Executors
        .newFixedThreadPool(4, ThreadFactoryUtils.build("prefetch-%d", true)),
        new NoopUfsAbsentPathCache(), UfsAbsentPathCache.ALWAYS);
    MountTable mountTable = mFileSystemMaster.getMountTable();

    String dir0 = TEST_DIR_PREFIX + "0";
    createUfsDir(dir0);
    createUfsFile(dir0 + TEST_FILE_PREFIX + "0").close();
    createUfsFile(dir0 + TEST_FILE_PREFIX + "1").close();
    createUfsFile(dir0 + TEST_FILE_PREFIX + "2").close();
    String dir1 = TEST_DIR_PREFIX + "1";
    createUfsFile(dir1 + TEST_FILE_PREFIX + "0").close();
    createUfsFile(dir1 + TEST_FILE_PREFIX + "1").close();
    createUfsFile(dir1 + TEST_FILE_PREFIX + "2").close();
    String dir2 = TEST_DIR_PREFIX + "2";

    //The path is existent in UFS
    //The prefetch should succeed
    ufsStatusCache.prefetchChildren(ROOT, mountTable);
    ufsStatusCache.fetchChildrenIfAbsent(null, new AlluxioURI("/"), mountTable, false);
    assertEquals(previousPrefetchOpsCount + 1, prefetchOpsCount.getCount());
    previousPrefetchOpsCount += 1;
    assertEquals(previousSucceededPrefetches + 1, succeededPrefetches.getCount());
    previousSucceededPrefetches += 1;
    assertEquals(previousFailedPrefetches + 0, failedPrefetches.getCount());
    previousFailedPrefetches += 0;
    // "/dir0" , "/dir1"
    assertEquals(previousPrefetchPaths + 2, prefetchPaths.getCount());
    previousPrefetchPaths += 2;
    assertEquals(previousCancelPrefetches + 0, canceledPrefetches.getCount());
    previousCancelPrefetches += 0;
    ufsStatusCache.remove(ROOT);

    //The path is non-existent in UFS
    //The prefetch should succeed
    ufsStatusCache.prefetchChildren(new AlluxioURI(dir2), mountTable);
    ufsStatusCache.fetchChildrenIfAbsent(null, new AlluxioURI(dir2), mountTable, false);
    assertEquals(previousPrefetchOpsCount + 1, prefetchOpsCount.getCount());
    previousPrefetchOpsCount += 1;
    assertEquals(previousSucceededPrefetches + 1, succeededPrefetches.getCount());
    previousSucceededPrefetches += 1;
    assertEquals(previousFailedPrefetches + 0, failedPrefetches.getCount());
    previousFailedPrefetches += 0;
    assertEquals(previousPrefetchPaths + 0, prefetchPaths.getCount());
    previousPrefetchPaths += 0;
    assertEquals(previousCancelPrefetches + 0, canceledPrefetches.getCount());
    previousCancelPrefetches += 0;
    ufsStatusCache.remove(new AlluxioURI(dir2));

    //The path is existent in UFS but UFS throws IOException
    //The prefetch should succeed
    mUfs.mIsThrowIOException = true;
    ufsStatusCache.prefetchChildren(ROOT, mountTable);
    ufsStatusCache.fetchChildrenIfAbsent(null, ROOT, mountTable, false);
    assertEquals(previousPrefetchOpsCount + 1, prefetchOpsCount.getCount());
    previousPrefetchOpsCount += 1;
    assertEquals(previousSucceededPrefetches + 1, succeededPrefetches.getCount());
    previousSucceededPrefetches += 1;
    assertEquals(previousFailedPrefetches + 0, failedPrefetches.getCount());
    previousFailedPrefetches += 0;
    assertEquals(previousPrefetchPaths + 0, prefetchPaths.getCount());
    previousPrefetchPaths += 0;
    assertEquals(previousCancelPrefetches + 0, canceledPrefetches.getCount());
    previousCancelPrefetches += 0;
    ufsStatusCache.remove(ROOT);

    //The path is existent in UFS and UFS throws RuntimeException
    //The prefetch should fail
    mUfs.mIsThrowRunTimeException = true;
    ufsStatusCache.prefetchChildren(ROOT, mountTable);
    ufsStatusCache.fetchChildrenIfAbsent(null, ROOT, mountTable, false);
    assertEquals(previousPrefetchOpsCount + 1, prefetchOpsCount.getCount());
    previousPrefetchOpsCount += 1;
    assertEquals(previousSucceededPrefetches + 0, succeededPrefetches.getCount());
    previousSucceededPrefetches += 0;
    assertEquals(previousFailedPrefetches + 1, failedPrefetches.getCount());
    previousFailedPrefetches += 1;
    assertEquals(previousPrefetchPaths + 0, prefetchPaths.getCount());
    previousPrefetchPaths += 0;
    assertEquals(previousCancelPrefetches + 0, canceledPrefetches.getCount());
    previousCancelPrefetches += 0;
    ufsStatusCache.remove(ROOT);

    //The prefetchRetries should increase because the UFS.listStatus() is delayed
    mUfs.mIsThrowIOException = false;
    mUfs.mIsThrowRunTimeException = false;
    mUfs.mIsDelay = true;
    ufsStatusCache.prefetchChildren(ROOT, mountTable);
    ufsStatusCache.fetchChildrenIfAbsent(null, ROOT, mountTable, false);
    assertEquals(previousPrefetchOpsCount + 1, prefetchOpsCount.getCount());
    previousPrefetchOpsCount += 1;
    assertEquals(previousSucceededPrefetches + 1, succeededPrefetches.getCount());
    previousSucceededPrefetches += 1;
    assertEquals(previousFailedPrefetches + 0, failedPrefetches.getCount());
    previousFailedPrefetches += 0;
    // "/dir0" , "/dir1"
    assertEquals(previousPrefetchPaths + 2, prefetchPaths.getCount());
    previousPrefetchPaths += 2;
    assertEquals(previousCancelPrefetches + 0, canceledPrefetches.getCount());
    previousCancelPrefetches += 0;
    assertTrue(0 < prefetchRetries.getCount());
    ufsStatusCache.remove(ROOT);

    //The canceledPrefetch should increase because of double submission
    ufsStatusCache.prefetchChildren(ROOT, mountTable);
    ufsStatusCache.prefetchChildren(ROOT, mountTable);
    ufsStatusCache.fetchChildrenIfAbsent(null, ROOT, mountTable, false);
    assertEquals(previousPrefetchOpsCount + 2, prefetchOpsCount.getCount());
    previousPrefetchOpsCount += 2;
    assertEquals(previousSucceededPrefetches + 1, succeededPrefetches.getCount());
    previousSucceededPrefetches += 1;
    assertEquals(previousFailedPrefetches + 0, failedPrefetches.getCount());
    previousFailedPrefetches += 0;
    // "/dir0" , "/dir1"
    assertEquals(previousPrefetchPaths + 2, prefetchPaths.getCount());
    previousPrefetchPaths += 2;
    assertEquals(previousCancelPrefetches + 1, canceledPrefetches.getCount());
    previousCancelPrefetches += 1;
    ufsStatusCache.remove(ROOT);

    mUfs.mIsDelay = false;
    //Let the UFS is available
    //All prefetches should succeed
    LockingScheme syncScheme =
        new LockingScheme(ROOT, InodeTree.LockPattern.READ, true);
    InodeSyncStream inodeSyncStream =
        new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
            DescendantType.ALL, ListStatusContext.defaults().getOptions().getCommonOptions(),
            false, true, false, false);
    inodeSyncStream.sync();

    //getFromUfs: "/" , "/dir0" , "/dir0/file0" , "/dir0/file1" ,"/dir0/file2"
    //                  "/dir1" , "/dir1/file0" , "/dir1/file1" , "/dir1/file2"
    //prefetchChildren: "/" , "/dir0" , "/dir0" , "/dir1" , "/dir1"
    assertEquals(previousPrefetchOpsCount + 14, prefetchOpsCount.getCount());
    previousPrefetchOpsCount += 14;
    //getFromUfs: "/" , "/dir0" , "/dir0/file0" , "/dir0/file1" ,"/dir0/file2"
    //                  "/dir1" , "/dir1/file0" , "/dir1/file1" , "/dir1/file2"
    //prefetchChildren: "/" , "/dir0" , "/dir1"
    assertEquals(previousSucceededPrefetches + 12, succeededPrefetches.getCount());
    previousSucceededPrefetches += 12;
    //getFromUfs: "/" , "/dir0" , "/dir0/file0" , "/dir0/file1" ,"/dir0/file2"
    //                  "/dir1" , "/dir1/file0" , "/dir1/file1" , "/dir1/file2"
    //prefetchChildren: "/dir0" , "/dir0/file0" , "/dir0/file1" ,"/dir0/file2"
    //                  "/dir1" , "/dir1/file0" , "/dir1/file1" , "/dir1/file2"
    assertEquals(previousPrefetchPaths + 17, prefetchPaths.getCount());
    previousPrefetchPaths += 17;
    assertEquals(previousFailedPrefetches + 0, failedPrefetches.getCount());
    previousFailedPrefetches += 0;
    // "/dir0" and "/dir1" in prefetchChildren are canceled because of double commits
    assertEquals(previousCancelPrefetches + 2, canceledPrefetches.getCount());
    previousCancelPrefetches += 2;

    //Let UFS throw IOException
    mUfs.mIsThrowIOException = true;
    syncScheme =
        new LockingScheme(ROOT, InodeTree.LockPattern.READ, true);
    inodeSyncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.ALL, ListStatusContext.defaults().getOptions().getCommonOptions(),
        false, true, false, false);
    inodeSyncStream.sync();

    //getFromUfs: "/" , "/dir0" , "/dir1"
    //prefetchChildren: "/" , "/dir0" , "/dir1"
    assertEquals(previousPrefetchOpsCount + 6, prefetchOpsCount.getCount());
    previousPrefetchOpsCount += 6;
    //getFromUfs: "/" , "/dir0" , "/dir1"
    //prefetchChildren: "/"
    assertEquals(previousSucceededPrefetches + 4, succeededPrefetches.getCount());
    previousSucceededPrefetches += 4;
    //getFromUfs: "/" , "/dir0" , "/dir1"
    assertEquals(previousPrefetchPaths + 3, prefetchPaths.getCount());
    previousPrefetchPaths += 3;
    assertEquals(previousFailedPrefetches + 0, failedPrefetches.getCount());
    previousFailedPrefetches += 0;
    // "/dir0" and "/dir1" in prefetchChildren are canceled by cancelAllPrefetch
    // because the prefetches for "/dir0" and "/dir1" are submitted and then
    // "/dir0" and "/dir1" are removed from inodeTree when sync the root path.
    // So the prefetches are discarded
    assertEquals(previousCancelPrefetches + 2, canceledPrefetches.getCount());
    previousCancelPrefetches += 2;
  }

  @Test
  public void ufsStatusCacheSizeMetrics() {
    final Counter cacheSizeTotal = DefaultFileSystemMaster.Metrics.UFS_STATUS_CACHE_SIZE_TOTAL;
    final Counter cacheChildrenSizeTotal =
        DefaultFileSystemMaster.Metrics.UFS_STATUS_CACHE_CHILDREN_SIZE_TOTAL;
    UfsStatusCache ufsStatusCache = new UfsStatusCache(Executors
        .newSingleThreadExecutor(ThreadFactoryUtils.build("prefetch-%d", true)),
        new NoopUfsAbsentPathCache(), UfsAbsentPathCache.ALWAYS);

    AlluxioURI path0 = new AlluxioURI("/dir0");
    UfsStatus stat0 = createUfsStatusWithName("dir0");
    AlluxioURI path1 = new AlluxioURI("/dir1");

    ufsStatusCache.addStatus(path0, stat0);
    assertEquals(1, cacheSizeTotal.getCount());

    // add a path already in the cache
    ufsStatusCache.addStatus(path0, stat0);
    assertEquals(1, cacheSizeTotal.getCount());

    // path and status name mismatch
    assertThrows(IllegalArgumentException.class, () -> ufsStatusCache.addStatus(path1, stat0));
    assertEquals(1, cacheSizeTotal.getCount());

    ufsStatusCache.remove(path0);
    assertEquals(0, cacheSizeTotal.getCount());

    // remove a path that has been removed
    ufsStatusCache.remove(path0);
    assertEquals(0, cacheSizeTotal.getCount());

    // remove a path not present in cache
    ufsStatusCache.remove(path1);
    assertEquals(0, cacheSizeTotal.getCount());

    AlluxioURI path2 = new AlluxioURI("/dir2");
    UfsStatus stat2 = createUfsStatusWithName("dir2");
    ufsStatusCache.addStatus(path2, stat2);

    // add a 3-children list
    List<UfsStatus> statusList = ImmutableList.of("1", "2", "3")
        .stream()
        .map(FileSystemMasterSyncMetadataMetricsTest::createUfsStatusWithName)
        .collect(Collectors.toList());
    ufsStatusCache.addChildren(path2, statusList);
    assertEquals(4, cacheSizeTotal.getCount());
    assertEquals(3, cacheChildrenSizeTotal.getCount());

    // replace with a 4-children list
    statusList = ImmutableList.of("1", "2", "3", "4")
        .stream()
        .map(FileSystemMasterSyncMetadataMetricsTest::createUfsStatusWithName)
        .collect(Collectors.toList());
    ufsStatusCache.addChildren(path2, statusList);
    assertEquals(5, cacheSizeTotal.getCount());
    assertEquals(4, cacheChildrenSizeTotal.getCount());

    ufsStatusCache.remove(path2);
    assertEquals(0, cacheSizeTotal.getCount());
    assertEquals(0, cacheChildrenSizeTotal.getCount());

    // remove once more
    ufsStatusCache.remove(path2);
    assertEquals(0, cacheSizeTotal.getCount());
    assertEquals(0, cacheChildrenSizeTotal.getCount());
  }

  private void createUfsDir(String path) throws IOException {
    mUfs.mkdirs(PathUtils.concatPath(mUfsUri, path));
  }

  private OutputStream createUfsFile(String path) throws IOException {
    return mUfs.create(PathUtils.concatPath(mUfsUri, path));
  }

  private static UfsStatus createUfsStatusWithName(String name) {
    return new UfsFileStatus(name, "hash", 0, 0L, "owner", "group", (short) 0, null, 0);
  }

  private static class ExceptionThrowingLocalUnderFileSystem extends LocalUnderFileSystem {
    public boolean mIsThrowIOException = false;
    public boolean mIsThrowRunTimeException = false;
    public boolean mIsDelay = false;
    public long mDelayTime = 2L;

    public ExceptionThrowingLocalUnderFileSystem(AlluxioURI uri,
        UnderFileSystemConfiguration conf) {
      super(uri, conf);
    }

    @Override
    public UfsStatus getStatus(String path) throws IOException {
      if (mIsThrowRunTimeException) {
        throw new RuntimeException();
      }
      if (mIsThrowIOException) {
        throw new IOException();
      }
      if (mIsDelay) {
        try {
          Thread.sleep(mDelayTime * Constants.SECOND);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return super.getStatus(path);
    }

    @Override
    public UfsStatus[] listStatus(String path) throws IOException {
      if (mIsThrowRunTimeException) {
        throw new RuntimeException();
      }
      if (mIsThrowIOException) {
        throw new IOException();
      }
      if (mIsDelay) {
        try {
          Thread.sleep(mDelayTime * Constants.SECOND);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return super.listStatus(path);
    }
  }
}

