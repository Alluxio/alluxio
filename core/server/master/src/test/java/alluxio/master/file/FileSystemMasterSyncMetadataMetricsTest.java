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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.file.options.DescendantType;
import alluxio.grpc.FileSystemMasterCommonPOptions;
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
import java.util.Collection;
import java.util.List;
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
  private FlakyLocalUnderFileSystem mUfs;
  private ExecutorService mFileSystemExecutorService;
  private ExecutorService mUfsStateCacheExecutorService;
  private MasterRegistry mRegistry;
  private DefaultFileSystemMaster mFileSystemMaster;

  private InodeTree mInodeTree;

  @Before
  public void before() throws Exception {
    UserState us = UserState.Factory.create(Configuration.global());
    AuthenticatedClientUser.set(us.getUser().getName());

    mTempDir.create();
    mUfsUri = mTempDir.newFolder().getAbsolutePath();

    mUfs = new FlakyLocalUnderFileSystem(new AlluxioURI(mUfsUri),
        UnderFileSystemConfiguration.defaults(Configuration.global()));
    PowerMockito.mockStatic(UnderFileSystem.Factory.class);
    Mockito.when(UnderFileSystem.Factory.create(anyString(), any())).thenReturn(mUfs);

    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
    Configuration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mUfsUri);
    String journalFolderUri = mTempDir.newFolder().getAbsolutePath();

    mFileSystemExecutorService = Executors
        .newFixedThreadPool(4, ThreadFactoryUtils.build("FileSystemMaster-%d", true));
    mUfsStateCacheExecutorService = Executors
        .newFixedThreadPool(4, ThreadFactoryUtils.build("UfsStateCache-%d", true));
    mRegistry = new MasterRegistry();
    JournalSystem journalSystem =
        JournalTestUtils.createJournalSystem(journalFolderUri);
    CoreMasterContext context = MasterTestUtils.testMasterContext(journalSystem);
    new MetricsMasterFactory().create(mRegistry, context);
    BlockMaster blockMaster = new BlockMasterFactory().create(mRegistry, context);
    mFileSystemMaster = new DefaultFileSystemMaster(blockMaster, context,
        ExecutorServiceFactories.constantExecutorServiceFactory(mFileSystemExecutorService));
    mInodeTree = mFileSystemMaster.getInodeTree();
    mRegistry.add(FileSystemMaster.class, mFileSystemMaster);
    journalSystem.start();
    journalSystem.gainPrimacy();
    mRegistry.start(true);

    MetricsSystem.resetAllMetrics();
  }

  @After
  public void after() throws Exception {
    mRegistry.stop();
    mFileSystemExecutorService.shutdown();
    mUfsStateCacheExecutorService.shutdown();
  }

  @Test
  public void metadataSyncMetrics() throws Exception {
    final Counter streamCountCounter =
        DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_COUNT;
    final Counter succeededStreamCounter =
        DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SUCCESS;
    final Counter failedStreamCounter =
        DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_FAIL;
    final Counter noChangePathsCounter =
        DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_NO_CHANGE;
    final Counter skippedStreamCounter =
        DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SKIPPED;
    final Counter succeededPathCounter =
        DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SYNC_PATHS_SUCCESS;
    final Counter failedPathCounter =
        DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SYNC_PATHS_FAIL;

    int streamCount = 0;
    int succeededStreams = 0;
    int failedStreams = 0;
    int noChangePaths = 0;
    int skippedStreams = 0;
    int succeededPaths = 0;
    int failedPaths = 0;

    final int dirNum = 10;
    final int fileNum = 20;

    // prepare test files in UFS
    for (int i = 0; i < dirNum; i++) {
      String dir = TEST_DIR_PREFIX + i;
      createUfsDir(dir);
      for (int j = 0; j < fileNum; j++) {
        createUfsFile(dir + TEST_FILE_PREFIX + j).close();
      }
    }

    // verify the files don't exist in alluxio
    assertEquals(1, mFileSystemMaster.getInodeTree().getInodeCount());

    FileSystemMasterCommonPOptions options =
        FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build();
    // force sync the root path
    // the sync should succeed
    LockingScheme syncScheme =
        new LockingScheme(ROOT, InodeTree.LockPattern.READ,
            true); // shouldSync
    InodeSyncStream syncStream =
        new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
            DescendantType.ALL, options,
            false, // isGetFileInfo
            false, // forceSync
            false, // loadOnly
            false); // loadAlways
    syncStream.sync();

    // verify the files exist in alluxio
    assertEquals(succeededPaths + (1 + dirNum * (1 + fileNum)), mInodeTree.getInodeCount());
    assertEquals(streamCount + 1, streamCountCounter.getCount());
    streamCount += 1;
    assertEquals(succeededStreams + 1, succeededStreamCounter.getCount());
    succeededStreams += 1;
    // "/" , "/dir*" and "/dir*/file*"
    assertEquals(succeededPaths + (1 + dirNum * (1 + fileNum)),
        succeededPathCounter.getCount());
    succeededPaths += (1 + dirNum * (1 + fileNum));
    assertEquals(failedStreams + 0, failedStreamCounter.getCount());
    failedStreams += 0;
    assertEquals(failedPaths + 0, failedPathCounter.getCount());
    failedPaths += 0;
    // "/" , "/dir*" and "/dir*/file*"
    assertEquals(noChangePaths + (1 + dirNum * (1 + fileNum)),
        noChangePathsCounter.getCount());
    noChangePaths += (1 + dirNum * (1 + fileNum));
    assertEquals(skippedStreams + 0, skippedStreamCounter.getCount());
    skippedStreams += 0;

    String path = TEST_DIR_PREFIX + "0" + TEST_FILE_PREFIX + "0";
    // overwrite the path(/dir0/file0) in UFS and force sync it
    // the sync should succeed and pick up the change
    OutputStream outputStream = createUfsFile(path);
    outputStream.write(new byte[] {0, 1});
    outputStream.close();
    syncScheme = new LockingScheme(new AlluxioURI(path), InodeTree.LockPattern.READ,
        true); // shouldSync
    syncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.NONE, options,
        false, // isGetFileInfo
        false, // forceSync
        false, // loadOnly
        false); // loadAlways
    syncStream.sync();
    assertEquals(2,
        mFileSystemMaster.getFileInfo(
            mFileSystemMaster.getFileId(new AlluxioURI(path))).getLength());
    assertEquals(streamCount + 1, streamCountCounter.getCount());
    streamCount += 1;
    assertEquals(succeededStreams + 1, succeededStreamCounter.getCount());
    succeededStreams += 1;
    assertEquals(succeededPaths + 1, succeededPathCounter.getCount());
    succeededPaths += 1;
    assertEquals(failedStreams + 0, failedStreamCounter.getCount());
    failedStreams += 0;
    assertEquals(failedPaths + 0, failedPathCounter.getCount());
    failedPaths += 0;
    assertEquals(noChangePaths + 0, noChangePathsCounter.getCount());
    noChangePaths += 0;
    assertEquals(skippedStreams + 0, skippedStreamCounter.getCount());
    skippedStreams += 0;

    // sync the path(/dir0/file0) again
    // the sync should succeed with no change
    syncScheme = new LockingScheme(new AlluxioURI(path), InodeTree.LockPattern.READ,
        true); // shouldSync
    syncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.NONE, options,
        false, // isGetFileInfo
        false, // forceSync
        false, // loadOnly
        false); // loadAlways
    syncStream.sync();
    assertTrue(mInodeTree.inodePathExists(new AlluxioURI(path)));
    assertEquals(streamCount + 1, streamCountCounter.getCount());
    streamCount += 1;
    assertEquals(succeededStreams + 1, succeededStreamCounter.getCount());
    succeededStreams += 1;
    assertEquals(succeededPaths + 1, succeededPathCounter.getCount());
    succeededPaths += 1;
    assertEquals(failedStreams + 0, failedStreamCounter.getCount());
    failedStreams += 0;
    assertEquals(failedPaths + 0, failedPathCounter.getCount());
    failedPaths += 0;
    assertEquals(noChangePaths + 1, noChangePathsCounter.getCount());
    noChangePaths += 1;
    assertEquals(skippedStreams + 0, skippedStreamCounter.getCount());
    skippedStreams += 0;

    // sync the path(/dir0/file0) again, the attempt is not shouldSync and forced
    // so should be skipped
    syncScheme = new LockingScheme(new AlluxioURI(path), InodeTree.LockPattern.READ,
        false); // shouldSync
    syncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.NONE, options,
        false, // isGetFileInfo
        false, // forceSync
        false, // loadOnly
        false); // loadAlways
    syncStream.sync();
    assertTrue(mInodeTree.inodePathExists(new AlluxioURI(path)));
    assertEquals(streamCount + 1, streamCountCounter.getCount());
    streamCount += 1;
    assertEquals(succeededStreams + 0, succeededStreamCounter.getCount());
    succeededStreams += 0;
    assertEquals(succeededPaths + 0, succeededPathCounter.getCount());
    succeededPaths += 0;
    assertEquals(failedStreams + 0, failedStreamCounter.getCount());
    failedStreams += 0;
    assertEquals(failedPaths + 0, failedPathCounter.getCount());
    failedPaths += 0;
    assertEquals(noChangePaths + 0, noChangePathsCounter.getCount());
    noChangePaths += 0;
    assertEquals(skippedStreams + 1, skippedStreamCounter.getCount());
    skippedStreams += 1;

    // simulate the case when the UFS throws IOException on the path
    // the sync should succeed and the path /dir0/file0 should be removed from inodeTree
    mUfs.mThrowIOException = true;
    syncScheme = new LockingScheme(new AlluxioURI(path), InodeTree.LockPattern.READ,
        true); // shouldSync
    syncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.NONE, options,
        false, // isGetFileInfo
        false, // forceSync
        false, // loadOnly
        false); // loadAlways
    syncStream.sync();
    assertFalse(mInodeTree.inodePathExists(new AlluxioURI(path)));
    assertEquals(streamCount + 1, streamCountCounter.getCount());
    streamCount += 1;
    assertEquals(succeededStreams + 1, succeededStreamCounter.getCount());
    succeededStreams += 1;
    assertEquals(succeededPaths + 1, succeededPathCounter.getCount());
    succeededPaths += 1;
    assertEquals(failedStreams + 0, failedStreamCounter.getCount());
    failedStreams += 0;
    assertEquals(failedPaths + 0, failedPathCounter.getCount());
    failedPaths += 0;
    assertEquals(noChangePaths + 0, noChangePathsCounter.getCount());
    noChangePaths += 0;
    assertEquals(skippedStreams + 0, skippedStreamCounter.getCount());
    skippedStreams += 0;

    // now the path(/dir0/file0) is non-existent in inodeTree and existent in UFS
    // the sync should fail because UFS throws IOException
    mUfs.mThrowIOException = true;
    syncScheme = new LockingScheme(new AlluxioURI(path), InodeTree.LockPattern.READ,
        true); // shouldSync
    syncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.NONE, options,
        false, // isGetFileInfo
        false, // forceSync
        false, // loadOnly
        false); // loadAlways
    syncStream.sync();
    assertFalse(mInodeTree.inodePathExists(new AlluxioURI(path)));
    assertEquals(streamCount + 1, streamCountCounter.getCount());
    streamCount += 1;
    assertEquals(succeededStreams + 0, succeededStreamCounter.getCount());
    succeededStreams += 0;
    assertEquals(succeededPaths + 0, succeededPathCounter.getCount());
    succeededPaths += 0;
    assertEquals(failedStreams + 1, failedStreamCounter.getCount());
    failedStreams += 1;
    assertEquals(failedPaths + 1, failedPathCounter.getCount());
    failedPaths += 1;
    assertEquals(noChangePaths + 0, noChangePathsCounter.getCount());
    noChangePaths += 0;
    assertEquals(skippedStreams + 0, skippedStreamCounter.getCount());
    skippedStreams += 0;

    // the path(/dir/file) are non-existent in inodeTree and UFS
    // the sync should fail
    mUfs.mThrowIOException = false;
    syncScheme = new LockingScheme(new AlluxioURI(TEST_DIR_PREFIX + dirNum + TEST_FILE_PREFIX),
        InodeTree.LockPattern.READ,
        true); // shouldSync
    syncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.NONE, options,
        false, // isGetFileInfo
        false, // forceSync
        false, // loadOnly
        false); // loadAlways
    syncStream.sync();
    assertFalse(mInodeTree.inodePathExists(new AlluxioURI(path)));
    assertEquals(streamCount + 1, streamCountCounter.getCount());
    streamCount += 1;
    assertEquals(succeededStreams + 0, succeededStreamCounter.getCount());
    succeededStreams += 0;
    assertEquals(succeededPaths + 0, succeededPathCounter.getCount());
    succeededPaths += 0;
    assertEquals(failedStreams + 1, failedStreamCounter.getCount());
    failedStreams += 1;
    assertEquals(failedPaths + 1, failedPathCounter.getCount());
    failedPaths += 1;
    assertEquals(noChangePaths + 0, noChangePathsCounter.getCount());
    noChangePaths += 0;
    assertEquals(skippedStreams + 0, skippedStreamCounter.getCount());
    skippedStreams += 0;
  }

  @Test
  public void metadataPrefetchMetrics() throws Exception {
    final Counter prefetchOpsCountCounter =
        DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_OPS_COUNT;
    final Counter succeededPrefetchCounter =
        DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_SUCCESS;
    final Counter failedPrefetchCounter =
        DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_FAIL;
    final Counter canceledPrefetchCounter =
        DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_CANCEL;
    final Counter prefetchPathsCounter =
        DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_PATHS;
    final Counter prefetchRetriesCounter =
        DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_RETRIES;

    int previousPrefetchOpsCount = 0;
    int previousSucceededPrefetches = 0;
    int previousFailedPrefetches = 0;
    int previousCancelPrefetches = 0;
    int previousPrefetchPaths = 0;

    UfsStatusCache ufsStatusCache = new UfsStatusCache(mUfsStateCacheExecutorService,
        mFileSystemMaster.getAbsentPathCache(), UfsAbsentPathCache.ALWAYS);
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

    // the path is existent in UFS
    // the prefetch should succeed
    ufsStatusCache.prefetchChildren(ROOT, mountTable);
    Collection<UfsStatus> ufsStatusCollection =
        ufsStatusCache.fetchChildrenIfAbsent(null, ROOT, mountTable, false);
    assertNotNull(ufsStatusCollection);
    assertEquals(2, ufsStatusCollection.size());
    assertEquals(previousPrefetchOpsCount + 1, prefetchOpsCountCounter.getCount());
    previousPrefetchOpsCount += 1;
    assertEquals(previousSucceededPrefetches + 1, succeededPrefetchCounter.getCount());
    previousSucceededPrefetches += 1;
    assertEquals(previousFailedPrefetches + 0, failedPrefetchCounter.getCount());
    previousFailedPrefetches += 0;
    // "/dir0" , "/dir1"
    assertEquals(previousPrefetchPaths + 2, prefetchPathsCounter.getCount());
    previousPrefetchPaths += 2;
    assertEquals(previousCancelPrefetches + 0, canceledPrefetchCounter.getCount());
    previousCancelPrefetches += 0;
    ufsStatusCache.remove(ROOT);

    // the path dir2 is non-existent in UFS
    // the prefetch should succeed
    ufsStatusCache.prefetchChildren(new AlluxioURI(dir2), mountTable);
    ufsStatusCollection =
        ufsStatusCache.fetchChildrenIfAbsent(null, new AlluxioURI(dir2), mountTable, false);
    assertNull(ufsStatusCollection);
    assertEquals(previousPrefetchOpsCount + 1, prefetchOpsCountCounter.getCount());
    previousPrefetchOpsCount += 1;
    assertEquals(previousSucceededPrefetches + 1, succeededPrefetchCounter.getCount());
    previousSucceededPrefetches += 1;
    assertEquals(previousFailedPrefetches + 0, failedPrefetchCounter.getCount());
    previousFailedPrefetches += 0;
    assertEquals(previousPrefetchPaths + 0, prefetchPathsCounter.getCount());
    previousPrefetchPaths += 0;
    assertEquals(previousCancelPrefetches + 0, canceledPrefetchCounter.getCount());
    previousCancelPrefetches += 0;
    ufsStatusCache.remove(new AlluxioURI(dir2));

    // the path is existent in UFS but UFS throws IOException
    // the prefetch should succeed
    mUfs.mThrowIOException = true;
    ufsStatusCache.prefetchChildren(ROOT, mountTable);
    ufsStatusCollection =
        ufsStatusCache.fetchChildrenIfAbsent(null, ROOT, mountTable, false);
    assertNull(ufsStatusCollection);
    assertEquals(previousPrefetchOpsCount + 1, prefetchOpsCountCounter.getCount());
    previousPrefetchOpsCount += 1;
    assertEquals(previousSucceededPrefetches + 1, succeededPrefetchCounter.getCount());
    previousSucceededPrefetches += 1;
    assertEquals(previousFailedPrefetches + 0, failedPrefetchCounter.getCount());
    previousFailedPrefetches += 0;
    assertEquals(previousPrefetchPaths + 0, prefetchPathsCounter.getCount());
    previousPrefetchPaths += 0;
    assertEquals(previousCancelPrefetches + 0, canceledPrefetchCounter.getCount());
    previousCancelPrefetches += 0;
    ufsStatusCache.remove(ROOT);

    // the path is existent in UFS and UFS throws RuntimeException
    // the prefetch should fail
    mUfs.mThrowRuntimeException = true;
    ufsStatusCache.prefetchChildren(ROOT, mountTable);
    ufsStatusCollection =
        ufsStatusCache.fetchChildrenIfAbsent(null, ROOT, mountTable, false);
    assertNull(ufsStatusCollection);
    assertEquals(previousPrefetchOpsCount + 1, prefetchOpsCountCounter.getCount());
    previousPrefetchOpsCount += 1;
    assertEquals(previousSucceededPrefetches + 0, succeededPrefetchCounter.getCount());
    previousSucceededPrefetches += 0;
    assertEquals(previousFailedPrefetches + 1, failedPrefetchCounter.getCount());
    previousFailedPrefetches += 1;
    assertEquals(previousPrefetchPaths + 0, prefetchPathsCounter.getCount());
    previousPrefetchPaths += 0;
    assertEquals(previousCancelPrefetches + 0, canceledPrefetchCounter.getCount());
    previousCancelPrefetches += 0;
    ufsStatusCache.remove(ROOT);

    // the prefetchRetries should increase because the UFS.listStatus() is delayed
    mUfs.mThrowIOException = false;
    mUfs.mThrowRuntimeException = false;
    mUfs.mIsSlow = true;
    ufsStatusCache.prefetchChildren(ROOT, mountTable);
    ufsStatusCollection =
        ufsStatusCache.fetchChildrenIfAbsent(null, ROOT, mountTable, false);
    assertNotNull(ufsStatusCollection);
    assertEquals(2, ufsStatusCollection.size());
    assertEquals(previousPrefetchOpsCount + 1, prefetchOpsCountCounter.getCount());
    previousPrefetchOpsCount += 1;
    assertEquals(previousSucceededPrefetches + 1, succeededPrefetchCounter.getCount());
    previousSucceededPrefetches += 1;
    assertEquals(previousFailedPrefetches + 0, failedPrefetchCounter.getCount());
    previousFailedPrefetches += 0;
    // "/dir0" , "/dir1"
    assertEquals(previousPrefetchPaths + 2, prefetchPathsCounter.getCount());
    previousPrefetchPaths += 2;
    assertEquals(previousCancelPrefetches + 0, canceledPrefetchCounter.getCount());
    previousCancelPrefetches += 0;
    assertTrue(0 < prefetchRetriesCounter.getCount());
    ufsStatusCache.remove(ROOT);

    // the 1st prefetch is cancelled because of double submission
    ufsStatusCache.prefetchChildren(ROOT, mountTable);
    ufsStatusCache.prefetchChildren(ROOT, mountTable);
    ufsStatusCollection =
        ufsStatusCache.fetchChildrenIfAbsent(null, ROOT, mountTable, false);
    assertNotNull(ufsStatusCollection);
    assertEquals(2, ufsStatusCollection.size());
    assertEquals(previousPrefetchOpsCount + 2, prefetchOpsCountCounter.getCount());
    previousPrefetchOpsCount += 2;
    assertEquals(previousSucceededPrefetches + 1, succeededPrefetchCounter.getCount());
    previousSucceededPrefetches += 1;
    assertEquals(previousFailedPrefetches + 0, failedPrefetchCounter.getCount());
    previousFailedPrefetches += 0;
    // "/dir0" , "/dir1"
    assertEquals(previousPrefetchPaths + 2, prefetchPathsCounter.getCount());
    previousPrefetchPaths += 2;
    assertEquals(previousCancelPrefetches + 1, canceledPrefetchCounter.getCount());
    previousCancelPrefetches += 1;
    ufsStatusCache.remove(ROOT);

    mUfs.mIsSlow = false;
    // when the UFS is available
    // all prefetches should succeed
    LockingScheme syncScheme =
        new LockingScheme(ROOT, InodeTree.LockPattern.READ, true);
    InodeSyncStream inodeSyncStream =
        new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
            DescendantType.ALL, ListStatusContext.defaults().getOptions().getCommonOptions(),
            false, true, false, false);
    inodeSyncStream.sync();
    assertEquals(9, mInodeTree.getInodeCount());
    // getFromUfs: "/" , "/dir0" , "/dir0/file0" , "/dir0/file1" ,"/dir0/file2"
    //                  "/dir1" , "/dir1/file0" , "/dir1/file1" , "/dir1/file2"
    // prefetchChildren: "/" , "/dir0" , "/dir0" , "/dir1" , "/dir1"
    assertEquals(previousPrefetchOpsCount + 14, prefetchOpsCountCounter.getCount());
    previousPrefetchOpsCount += 14;
    // getFromUfs: "/" , "/dir0" , "/dir0/file0" , "/dir0/file1" ,"/dir0/file2"
    //                  "/dir1" , "/dir1/file0" , "/dir1/file1" , "/dir1/file2"
    // prefetchChildren: "/" , "/dir0" , "/dir1"
    assertEquals(previousSucceededPrefetches + 12, succeededPrefetchCounter.getCount());
    previousSucceededPrefetches += 12;
    // getFromUfs: "/" , "/dir0" , "/dir0/file0" , "/dir0/file1" ,"/dir0/file2"
    //                  "/dir1" , "/dir1/file0" , "/dir1/file1" , "/dir1/file2"
    // prefetchChildren: "/dir0" , "/dir0/file0" , "/dir0/file1" ,"/dir0/file2"
    //                  "/dir1" , "/dir1/file0" , "/dir1/file1" , "/dir1/file2"
    assertEquals(previousPrefetchPaths + 17, prefetchPathsCounter.getCount());
    previousPrefetchPaths += 17;
    assertEquals(previousFailedPrefetches + 0, failedPrefetchCounter.getCount());
    previousFailedPrefetches += 0;
    // "/dir0" and "/dir1" in prefetchChildren are canceled because of double commits
    assertEquals(previousCancelPrefetches + 2, canceledPrefetchCounter.getCount());
    previousCancelPrefetches += 2;

    // When UFS throw IOException
    mUfs.mThrowIOException = true;
    syncScheme =
        new LockingScheme(ROOT, InodeTree.LockPattern.READ, true);
    inodeSyncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.ALL, ListStatusContext.defaults().getOptions().getCommonOptions(),
        false, true, false, false);
    inodeSyncStream.sync();
    assertEquals(1, mInodeTree.getInodeCount());
    // getFromUfs: "/" , "/dir0" , "/dir1"
    // prefetchChildren: "/" , "/dir0" , "/dir1"
    assertEquals(previousPrefetchOpsCount + 6, prefetchOpsCountCounter.getCount());
    previousPrefetchOpsCount += 6;
    // getFromUfs: "/" , "/dir0" , "/dir1"
    // prefetchChildren: "/"
    assertEquals(previousSucceededPrefetches + 4, succeededPrefetchCounter.getCount());
    previousSucceededPrefetches += 4;
    // getFromUfs: "/" , "/dir0" , "/dir1"
    assertEquals(previousPrefetchPaths + 3, prefetchPathsCounter.getCount());
    previousPrefetchPaths += 3;
    assertEquals(previousFailedPrefetches + 0, failedPrefetchCounter.getCount());
    previousFailedPrefetches += 0;
    // "/dir0" and "/dir1" in prefetchChildren are canceled by cancelAllPrefetch
    // because the prefetches for "/dir0" and "/dir1" are submitted and then
    // "/dir0" and "/dir1" are removed from inodeTree when sync the root path.
    // so these prefetches are discarded
    assertEquals(previousCancelPrefetches + 2, canceledPrefetchCounter.getCount());
    previousCancelPrefetches += 2;
  }

  @Test
  public void ufsStatusCacheSizeMetrics() {
    final Counter cacheSizeTotal = DefaultFileSystemMaster.Metrics.UFS_STATUS_CACHE_SIZE_TOTAL;
    final Counter cacheChildrenSizeTotal =
        DefaultFileSystemMaster.Metrics.UFS_STATUS_CACHE_CHILDREN_SIZE_TOTAL;
    UfsStatusCache ufsStatusCache = new UfsStatusCache(mUfsStateCacheExecutorService,
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

  private static class FlakyLocalUnderFileSystem extends LocalUnderFileSystem {
    public boolean mThrowIOException = false;
    public boolean mThrowRuntimeException = false;
    public boolean mIsSlow = false;
    public long mSlowTime = 2L;

    public FlakyLocalUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration conf) {
      super(uri, conf);
    }

    @Override
    public UfsStatus getStatus(String path) throws IOException {
      if (mThrowRuntimeException) {
        throw new RuntimeException();
      }
      if (mThrowIOException) {
        throw new IOException();
      }
      if (mIsSlow) {
        try {
          Thread.sleep(mSlowTime * Constants.SECOND);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return super.getStatus(path);
    }

    @Override
    public UfsStatus[] listStatus(String path) throws IOException {
      if (mThrowRuntimeException) {
        throw new RuntimeException();
      }
      if (mThrowIOException) {
        throw new IOException();
      }
      if (mIsSlow) {
        try {
          Thread.sleep(mSlowTime * Constants.SECOND);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return super.listStatus(path);
    }
  }
}

