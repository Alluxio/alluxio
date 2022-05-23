package alluxio.master.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.file.options.DescendantType;
import alluxio.grpc.MountPOptions;
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
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.master.journal.JournalType;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.user.UserState;
import alluxio.underfs.MasterUfsManager;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UfsStatusCache;
import alluxio.underfs.UfsStatusCacheTest;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.local.LocalUnderFileSystem;
import alluxio.util.IdUtils;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UnderFileSystem.Factory.class})
public class FileSystemMasterMetaDataSyncMetricTest {
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  private String mUfsUri;
  private ExceptionThrowingLocalUnderFileSystem mUfs;
  private ExecutorService mExecutorService;
  private MasterRegistry mRegistry;
  private DefaultFileSystemMaster mFileSystemMaster;

  @Before
  public void before() throws Exception {
    UserState s = UserState.Factory.create(ServerConfiguration.global());
    AuthenticatedClientUser.set(s.getUser().getName());

    mTempDir.create();
    mUfsUri = mTempDir.newFolder().getAbsolutePath();

    mUfs = new ExceptionThrowingLocalUnderFileSystem(new AlluxioURI(mUfsUri),
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global()));
    PowerMockito.mockStatic(UnderFileSystem.Factory.class);
    Mockito.when(UnderFileSystem.Factory.create(anyString(), any())).thenReturn(mUfs);

    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
    ServerConfiguration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mUfsUri);
    String journalFolderUri = mTempDir.newFolder().getAbsolutePath();

    mExecutorService = Executors
        .newSingleThreadExecutor(ThreadFactoryUtils.build("DefaultFileSystemMasterTest-%d", true));
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
  public void testMetadataSyncMetrics() throws Exception {
    final Counter count = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_COUNT;
    final Counter success = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SUCCESS;
    final Counter fail = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_FAIL;
    final Counter noChange = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_NO_CHANGE;
    final Counter skipped = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SKIPPED;
    final Counter pathSuccess =
        DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SYNC_PATHS_SUCCESS;
    final Counter pathFail = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SYNC_PATHS_FAIL;
    final Counter timeMS = DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_TIME_MS;

    createDir("/dir0");
    OutputStream outputStream = createFile("/dir0/file0");
    outputStream.write(new byte[] {0});
    outputStream.close();

    //path is non-existent in inodeTree,existent in UFS and UFS is available ->success and no change
    LockingScheme syncScheme =
        new LockingScheme(new AlluxioURI("/"), InodeTree.LockPattern.READ, true);
    InodeSyncStream inodeSyncStream =
        new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
            DescendantType.ALL, ListStatusContext.defaults().getOptions().getCommonOptions(),
            false, true, false, false);
    inodeSyncStream.sync();
    assertEquals(0 + 1, count.getCount());
    assertEquals(0 + 1, success.getCount());
    assertEquals(0 + 3, pathSuccess.getCount());
    assertEquals(0 + 0, fail.getCount());
    assertEquals(0 + 0, pathFail.getCount());
    assertEquals(0 + 3, noChange.getCount());
    assertEquals(0 + 0, skipped.getCount());

    //modify the file0
    //path is existent in inodeTree and UFS,but content is changed and UFS is available ->success
    outputStream = createFile("/dir0/file0");
    outputStream.write(new byte[] {0, 1});
    outputStream.close();
    syncScheme =
        new LockingScheme(new AlluxioURI("/dir0/file0"), InodeTree.LockPattern.READ, true);
    inodeSyncStream =
        new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
            DescendantType.NONE, ListStatusContext.defaults().getOptions().getCommonOptions(),
            false, true, false, false);
    inodeSyncStream.sync();
    assertEquals(1 + 1, count.getCount());
    assertEquals(1 + 1, success.getCount());
    assertEquals(3 + 1, pathSuccess.getCount());
    assertEquals(0 + 0, fail.getCount());
    assertEquals(0 + 0, pathFail.getCount());
    assertEquals(3 + 0, noChange.getCount());
    assertEquals(0 + 0, skipped.getCount());

    //sync "/dir0/file0" again
    //path is existent in inodeTree and UFS and UFS is available ->success and on change
    syncScheme =
        new LockingScheme(new AlluxioURI("/dir0/file0"), InodeTree.LockPattern.READ, true);
    inodeSyncStream =
        new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
            DescendantType.NONE, ListStatusContext.defaults().getOptions().getCommonOptions(),
            false, true, false, false);
    inodeSyncStream.sync();
    assertEquals(2 + 1, count.getCount());
    assertEquals(2 + 1, success.getCount());
    assertEquals(4 + 1, pathSuccess.getCount());
    assertEquals(0 + 0, fail.getCount());
    assertEquals(0 + 0, pathFail.getCount());
    assertEquals(3 + 1, noChange.getCount());
    assertEquals(0 + 0, skipped.getCount());

    //!forceSync && !shouldSync ->skip
    syncScheme =
        new LockingScheme(new AlluxioURI("/dir0/file0"), InodeTree.LockPattern.READ, false);
    inodeSyncStream =
        new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
            DescendantType.NONE, ListStatusContext.defaults().getOptions().getCommonOptions(),
            false,
            false, false, false);
    inodeSyncStream.sync();
    assertEquals(3 + 1, count.getCount());
    assertEquals(3 + 0, success.getCount());
    assertEquals(5 + 0, pathSuccess.getCount());
    assertEquals(0 + 0, fail.getCount());
    assertEquals(0 + 0, pathFail.getCount());
    assertEquals(4 + 0, noChange.getCount());
    assertEquals(0 + 1, skipped.getCount());

    //path is existent in inodeTree and UFS and UFS throws IOException -> success
    mUfs.mIsThrowIOException = true;
    syncScheme = new LockingScheme(new AlluxioURI("/dir0/file0"), InodeTree.LockPattern.READ, true);
    inodeSyncStream = new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
        DescendantType.NONE, ListStatusContext.defaults().getOptions().getCommonOptions(),
            false, true, false, false);
    inodeSyncStream.sync();
    assertFalse(mFileSystemMaster.getInodeTree().inodePathExists(new AlluxioURI("/dir0/file0")));
    assertEquals(4 + 1, count.getCount());
    assertEquals(3 + 1, success.getCount());
    assertEquals(5 + 1, pathSuccess.getCount());
    assertEquals(0 + 0, fail.getCount());
    assertEquals(0 + 0, pathFail.getCount());
    assertEquals(4 + 0, noChange.getCount());
    assertEquals(1 + 0, skipped.getCount());

    //path is non-existent in inodeTree,existent in UFS and UFS throws IOException -> fail
    syncScheme = new LockingScheme(new AlluxioURI("/dir0/file0"), InodeTree.LockPattern.READ, true);
    inodeSyncStream =
        new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
            DescendantType.NONE, ListStatusContext.defaults().getOptions().getCommonOptions(),
            false, true, false, false);
    inodeSyncStream.sync();
    assertEquals(5 + 1, count.getCount());
    assertEquals(4 + 0, success.getCount());
    assertEquals(6 + 0, pathSuccess.getCount());
    assertEquals(0 + 1, fail.getCount());
    assertEquals(0 + 1, pathFail.getCount());
    assertEquals(4 + 0, noChange.getCount());
    assertEquals(1 + 0, skipped.getCount());

    mUfs.mIsThrowIOException = false;
    //path is non-existent in inodeTree and UFS and UFS is available -> fail
    syncScheme = new LockingScheme(new AlluxioURI("/dir0/file0"), InodeTree.LockPattern.READ, true);
    inodeSyncStream =
        new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
            DescendantType.NONE, ListStatusContext.defaults().getOptions().getCommonOptions(),
            false, true, false, false);
    inodeSyncStream.sync();
    assertEquals(6 + 1, count.getCount());
    assertEquals(4 + 0, success.getCount());
    assertEquals(6 + 0, pathSuccess.getCount());
    assertEquals(1 + 1, fail.getCount());
    assertEquals(1 + 1, pathFail.getCount());
    assertEquals(4 + 0, noChange.getCount());
    assertEquals(1 + 0, skipped.getCount());
    assertTrue(timeMS.getCount() > 0);
  }

  @Test
  public void testMetadataPrefetchMetrics() throws Exception {
    Counter prefetchOpsCount = DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_OPS_COUNT;
    Counter prefetchSuccess = DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_SUCCESS;
    Counter prefetchFail = DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_FAIL;
    Counter prefetchCancel = DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_CANCEL;
    Counter prefetchPaths = DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_PATHS;
    Counter prefetchRetries = DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_RETRIES;
    UfsStatusCache ufsStatusCache = new UfsStatusCache(Executors
        .newSingleThreadExecutor(ThreadFactoryUtils.build("prefetch-%d", true)),
        new NoopUfsAbsentPathCache(), UfsAbsentPathCache.ALWAYS);
    MountTable mountTable = mFileSystemMaster.getMountTable();

    createDir("/dir0");
    createFile("/dir0/file0").close();

//    //path is existent in UFS and UFS is available -> prefetch success
//    ufsStatusCache.prefetchChildren(new AlluxioURI("/"), mountTable);
//    ufsStatusCache.fetchChildrenIfAbsent(null, new AlluxioURI("/"), mountTable, false);
//    assertEquals(0 + 1, prefetchOpsCount.getCount());
//    assertEquals(0 + 1, prefetchSuccess.getCount());
//    assertEquals(0 + 1, prefetchPaths.getCount());
//    assertEquals(0 + 0, prefetchFail.getCount());
//    assertEquals(0 + 0, prefetchCancel.getCount());
//    ufsStatusCache.remove(new AlluxioURI("/"));
//
//    //path is non-existent in UFS and UFS is available -> prefetch success
//    ufsStatusCache.prefetchChildren(new AlluxioURI("/dir2"), mountTable);
//    ufsStatusCache.fetchChildrenIfAbsent(null, new AlluxioURI("/dir2"), mountTable, false);
//    assertEquals(1 + 1, prefetchOpsCount.getCount());
//    assertEquals(1 + 1, prefetchSuccess.getCount());
//    assertEquals(1 + 0, prefetchPaths.getCount());
//    assertEquals(0 + 0, prefetchFail.getCount());
//    assertEquals(0 + 0, prefetchCancel.getCount());
//    ufsStatusCache.remove(new AlluxioURI("/dir2"));
//
//    //path is existent in UFS and UFS throws IOException -> prefetch success
//    mUfs.mIsThrowIOException = true;
//    ufsStatusCache.prefetchChildren(new AlluxioURI("/"), mountTable);
//    ufsStatusCache.fetchChildrenIfAbsent(null, new AlluxioURI("/"), mountTable, false);
//    assertEquals(2 + 1, prefetchOpsCount.getCount());
//    assertEquals(2 + 1, prefetchSuccess.getCount());
//    assertEquals(1 + 0, prefetchPaths.getCount());
//    assertEquals(0 + 0, prefetchFail.getCount());
//    assertEquals(0 + 0, prefetchCancel.getCount());
//    ufsStatusCache.remove(new AlluxioURI("/"));
//
//    //path is existent in UFS and UFS throws RuntimeException -> prefetch fail
//    mUfs.mIsThrowRunTimeException = true;
//    ufsStatusCache.prefetchChildren(new AlluxioURI("/"), mountTable);
//    ufsStatusCache.fetchChildrenIfAbsent(null, new AlluxioURI("/"), mountTable, false);
//    assertEquals(3 + 1, prefetchOpsCount.getCount());
//    assertEquals(3 + 0, prefetchSuccess.getCount());
//    assertEquals(1 + 0, prefetchPaths.getCount());
//    assertEquals(0 + 1, prefetchFail.getCount());
//    assertEquals(0 + 0, prefetchCancel.getCount());
//    ufsStatusCache.remove(new AlluxioURI("/"));
//
//    //delay the operation for retry
//    //path is existent in UFS and UFS is available -> prefetch success
//    mUfs.mIsThrowIOException = false;
//    mUfs.mIsThrowRunTimeException = false;
//    mUfs.mIsDelay = true;
//    ufsStatusCache.prefetchChildren(new AlluxioURI("/"), mountTable);
//    ufsStatusCache.fetchChildrenIfAbsent(null, new AlluxioURI("/"), mountTable, false);
//    assertEquals(4 + 1, prefetchOpsCount.getCount());
//    assertEquals(3 + 1, prefetchSuccess.getCount());
//    assertEquals(1 + 1, prefetchPaths.getCount());
//    assertEquals(1 + 0, prefetchFail.getCount());
//    assertEquals(0 + 0, prefetchCancel.getCount());
//    assertTrue(0 < prefetchRetries.getCount());
//    ufsStatusCache.remove(new AlluxioURI("/"));
//
//    //prefetch repeatedly for cancel
//    //path is existent in UFS and UFS is available -> prefetch success
//    ufsStatusCache.prefetchChildren(new AlluxioURI("/"), mountTable);
//    ufsStatusCache.prefetchChildren(new AlluxioURI("/"), mountTable);
//    ufsStatusCache.fetchChildrenIfAbsent(null, new AlluxioURI("/"), mountTable, false);
//    assertEquals(5 + 2, prefetchOpsCount.getCount());
//    assertEquals(4 + 1, prefetchSuccess.getCount());
//    assertEquals(2 + 1, prefetchPaths.getCount());
//    assertEquals(1 + 0, prefetchFail.getCount());
//    assertEquals(0 + 1, prefetchCancel.getCount());
//    ufsStatusCache.remove(new AlluxioURI("/"));

    //path is existent in inodeTree and UFS and UFS is available -> success
    LockingScheme syncScheme =
        new LockingScheme(new AlluxioURI("/"), InodeTree.LockPattern.READ, true);
    InodeSyncStream inodeSyncStream =
        new InodeSyncStream(syncScheme, mFileSystemMaster, RpcContext.NOOP,
            DescendantType.ALL, ListStatusContext.defaults().getOptions().getCommonOptions(),
            false, true, false, false);
    inodeSyncStream.sync();
    assertEquals(2, prefetchOpsCount.getCount());
    assertEquals(2, prefetchSuccess.getCount());
    assertEquals(2, prefetchPaths.getCount());
    assertEquals(0, prefetchFail.getCount());
    assertEquals(0, prefetchCancel.getCount());

  }

  @Test
  public void testUfsStatusCacheSizeMetrics() throws Exception{
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
        .map(FileSystemMasterMetaDataSyncMetricTest::createUfsStatusWithName)
        .collect(Collectors.toList());
    ufsStatusCache.addChildren(path2, statusList);
    assertEquals(4, cacheSizeTotal.getCount());
    assertEquals(3, cacheChildrenSizeTotal.getCount());

    // replace with a 4-children list
    statusList = ImmutableList.of("1", "2", "3", "4")
        .stream()
        .map(FileSystemMasterMetaDataSyncMetricTest::createUfsStatusWithName)
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

  private void createDir(String path) throws IOException {
    mUfs.mkdirs(PathUtils.concatPath(mUfsUri, path));
  }

  private OutputStream createFile(String path) throws IOException {
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
                                              UnderFileSystemConfiguration ufsConf) {
      super(uri, ufsConf);
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

