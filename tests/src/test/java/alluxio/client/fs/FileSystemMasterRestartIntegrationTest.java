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

package alluxio.client.fs;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.FreeContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.master.file.meta.TtlIntervalRule;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.master.FsMasterResource;
import alluxio.testutils.master.MasterTestUtils;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsMode;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Test behavior of {@link FileSystemMaster}.
 *
 * For example, (concurrently) creating/deleting/renaming files.
 */
public class FileSystemMasterRestartIntegrationTest extends BaseIntegrationTest {
  private static final int DEPTH = 6;
  private static final int FILES_PER_NODE = 4;
  private static final int CONCURRENCY_DEPTH = 3;
  private static final AlluxioURI ROOT_PATH = new AlluxioURI("/root");
  private static final AlluxioURI ROOT_PATH2 = new AlluxioURI("/root2");
  // Modify current time so that implementations can't accidentally pass unit tests by ignoring
  // this specified time and always using System.currentTimeMillis()
  private static final long TEST_TIME_MS = Long.MAX_VALUE;
  private static final long TTL_CHECKER_INTERVAL_MS = 1000;
  private static final String TEST_USER = "test";
  // Time to wait for shutting down thread pool.
  private static final long SHUTDOWN_TIME_MS = 15 * Constants.SECOND_MS;

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.MASTER_TTL_CHECK);

  @ClassRule
  public static TtlIntervalRule sTtlIntervalRule = new TtlIntervalRule(TTL_CHECKER_INTERVAL_MS);

  @Rule
  public Timeout mGlobalTimeout = Timeout.seconds(60);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false)
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS,
              String.valueOf(TTL_CHECKER_INTERVAL_MS))
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, 1000)
          .setProperty(PropertyKey.MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION, 0)
          .setProperty(PropertyKey.SECURITY_LOGIN_USERNAME, TEST_USER).build();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER,
      ServerConfiguration.global());

  private FileSystemMaster mFsMaster;

  @Before
  public final void before() throws Exception {
    mFsMaster = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class);
  }

  private FsMasterResource createFileSystemMasterFromJournal() throws Exception {
    return MasterTestUtils.createLeaderFileSystemMasterFromJournalCopy();
  }

  private long countPaths() throws Exception {
    long count = 1;
    Stack<AlluxioURI> dirs = new Stack();
    dirs.push(new AlluxioURI("/"));
    while (!dirs.isEmpty()) {
      AlluxioURI uri = dirs.pop();
      for (FileInfo child : mFsMaster.listStatus(uri, ListStatusContext.defaults())) {
        count++;
        AlluxioURI childUri = new AlluxioURI(PathUtils.concatPath(uri, child.getName()));
        if (mFsMaster.getFileInfo(childUri, GetStatusContext.defaults()).isFolder()) {
          dirs.push(childUri);
        }
      }
    }
    return count;
  }

  @Test
  public void syncReplay() throws Exception {
    AlluxioURI root = new AlluxioURI("/");
    AlluxioURI alluxioFile = new AlluxioURI("/in_alluxio");

    // Create a persisted Alluxio file (but no ufs file).
    mFsMaster.createFile(alluxioFile, CreateFileContext.defaults()
        .setWriteType(WriteType.CACHE_THROUGH));
    mFsMaster.completeFile(alluxioFile,
        CompleteFileContext.mergeFrom(CompleteFilePOptions.newBuilder().setUfsLength(0))
            .setOperationTimeMs(TEST_TIME_MS));

    // List what is in Alluxio, without syncing.
    List<FileInfo> files = mFsMaster.listStatus(root,
        ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
            .setLoadMetadataType(LoadMetadataPType.NEVER)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1))));
    Assert.assertEquals(1, files.size());
    Assert.assertEquals(alluxioFile.getName(), files.get(0).getName());

    // Add ufs only paths
    String ufs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Files.createDirectory(Paths.get(ufs, "ufs_dir"));
    Files.createFile(Paths.get(ufs, "ufs_file"));

    // List with syncing, which will remove alluxio only path, and add ufs only paths.
    files = mFsMaster.listStatus(root,
        ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
            .setLoadMetadataType(LoadMetadataPType.NEVER)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0))));
    Assert.assertEquals(2, files.size());
    Set<String> filenames = files.stream().map(FileInfo::getName).collect(Collectors.toSet());
    Assert.assertTrue(filenames.contains("ufs_dir"));
    Assert.assertTrue(filenames.contains("ufs_file"));

    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);

      // List what is in Alluxio, without syncing. Should match the last state.
      files = fsMaster.listStatus(root, ListStatusContext.mergeFrom(
          ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)
              .setCommonOptions(
                  FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1))));
      Assert.assertEquals(2, files.size());
      filenames = files.stream().map(FileInfo::getName).collect(Collectors.toSet());
      Assert.assertTrue(filenames.contains("ufs_dir"));
      Assert.assertTrue(filenames.contains("ufs_file"));
    }
  }

  @Test
  public void syncDirReplay() throws Exception {
    AlluxioURI dir = new AlluxioURI("/dir/");

    // Add ufs nested file.
    String ufs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Files.createDirectory(Paths.get(ufs, "dir"));
    Files.createFile(Paths.get(ufs, "dir", "file"));

    File ufsDir = new File(Paths.get(ufs, "dir").toString());
    Assert.assertTrue(ufsDir.setReadable(true, false));
    Assert.assertTrue(ufsDir.setWritable(true, false));
    Assert.assertTrue(ufsDir.setExecutable(true, false));

    // List dir with syncing
    FileInfo info = mFsMaster.getFileInfo(dir,
        GetStatusContext.mergeFrom(GetStatusPOptions.newBuilder()
            .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(
                FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build())));
    Assert.assertNotNull(info);
    Assert.assertEquals("dir", info.getName());
    // Retrieve the mode
    int mode = info.getMode();

    // Update mode of the ufs dir
    Assert.assertTrue(ufsDir.setExecutable(false, false));

    // List dir with syncing, should update the mode
    info = mFsMaster.getFileInfo(dir,
        GetStatusContext.mergeFrom(GetStatusPOptions.newBuilder()
            .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(
                FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build())));
    Assert.assertNotNull(info);
    Assert.assertEquals("dir", info.getName());
    Assert.assertNotEquals(mode, info.getMode());
    // update the expected mode
    mode = info.getMode();

    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);

      // List what is in Alluxio, without syncing.
      info = fsMaster.getFileInfo(dir, GetStatusContext.mergeFrom(
          GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)
              .setCommonOptions(
                  FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).build())));
      Assert.assertNotNull(info);
      Assert.assertEquals("dir", info.getName());
      Assert.assertEquals(mode, info.getMode());
    }
  }

  @Test
  public void unavailableUfsRecursiveCreate() throws Exception {
    String ufsBase = "test://test";

    UnderFileSystemFactory mockUfsFactory = Mockito.mock(UnderFileSystemFactory.class);
    Mockito.when(mockUfsFactory.supportsPath(Matchers.anyString(), Matchers.any()))
        .thenReturn(Boolean.FALSE);
    Mockito.when(mockUfsFactory.supportsPath(Matchers.eq(ufsBase), Matchers.any()))
        .thenReturn(Boolean.TRUE);

    UnderFileSystem mockUfs = Mockito.mock(UnderFileSystem.class);
    UfsDirectoryStatus ufsStatus = new
        UfsDirectoryStatus("test", "owner", "group", (short) 511);
    Mockito.when(mockUfsFactory.create(Matchers.eq(ufsBase), Matchers.any())).thenReturn(mockUfs);
    Mockito.when(mockUfs.isDirectory(ufsBase)).thenReturn(true);
    Mockito.when(mockUfs.resolveUri(new AlluxioURI(ufsBase), ""))
        .thenReturn(new AlluxioURI(ufsBase));
    Mockito.when(mockUfs.resolveUri(new AlluxioURI(ufsBase), "/dir1"))
        .thenReturn(new AlluxioURI(ufsBase + "/dir1"));
    Mockito.when(mockUfs.getExistingDirectoryStatus(ufsBase))
        .thenReturn(ufsStatus);
    Mockito.when(mockUfs.mkdirs(Matchers.eq(ufsBase + "/dir1"), Matchers.any()))
        .thenThrow(new IOException("ufs unavailable"));
    Mockito.when(mockUfs.getStatus(ufsBase))
        .thenReturn(ufsStatus);

    UnderFileSystemFactoryRegistry.register(mockUfsFactory);

    mFsMaster.mount(new AlluxioURI("/mnt"), new AlluxioURI(ufsBase), MountContext.defaults());

    AlluxioURI root = new AlluxioURI("/mnt/");
    AlluxioURI alluxioFile = new AlluxioURI("/mnt/dir1/dir2/file");

    // Create a persisted Alluxio file (but no ufs file).
    try {
      mFsMaster.createFile(alluxioFile, CreateFileContext
          .mergeFrom(CreateFilePOptions.newBuilder().setRecursive(true))
          .setWriteType(WriteType.CACHE_THROUGH));
      Assert.fail("persisted create should fail, when UFS is unavailable");
    } catch (Exception e) {
      // expected, ignore
    }

    List<FileInfo> files =
        mFsMaster.listStatus(root, ListStatusContext.defaults());

    Assert.assertTrue(files.isEmpty());

    try {
      // should not exist
      files = mFsMaster.listStatus(new AlluxioURI("/mnt/dir1/"),
          ListStatusContext.defaults());
      Assert.fail("dir should not exist, when UFS is unavailable");
    } catch (Exception e) {
      // expected, ignore
    }

    try {
      // should not exist
      mFsMaster.delete(new AlluxioURI("/mnt/dir1/"),
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      Assert.fail("cannot delete non-existing directory, when UFS is unavailable");
    } catch (Exception e) {
      // expected, ignore
      files = null;
    }

    files = mFsMaster.listStatus(new AlluxioURI("/mnt/"),
        ListStatusContext.defaults());
    Assert.assertTrue(files.isEmpty());

    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    try (FsMasterResource masterResource = MasterTestUtils
        .createLeaderFileSystemMasterFromJournal()) {
      FileSystemMaster newFsMaster = masterResource.getRegistry().get(FileSystemMaster.class);

      files = newFsMaster.listStatus(new AlluxioURI("/mnt/"),
          ListStatusContext.defaults());
      Assert.assertTrue(files.isEmpty());
    }
  }

  @Test
  public void ufsModeReplay() throws Exception {
    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.NO_ACCESS);

    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);

      AlluxioURI alluxioFile = new AlluxioURI("/in_alluxio");
      // Create file should throw an Exception even after restart
      mThrown.expect(AccessControlException.class);
      fsMaster.createFile(alluxioFile, CreateFileContext.defaults()
          .setWriteType(WriteType.CACHE_THROUGH));
    }
  }

  /**
   * Tests journal is updated with access time asynchronously before master is stopped.
   */
  @Test
  public void updateAccessTimeAsyncFlush() throws Exception {
    String parentName = "d1";
    AlluxioURI parentPath = new AlluxioURI("/" + parentName);
    long parentId = mFsMaster.createDirectory(parentPath,
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)
            .setMode(new Mode((short) 0700).toProto())));
    long oldAccessTime = mFsMaster.getFileInfo(parentId).getLastAccessTimeMs();
    Thread.sleep(100);
    mFsMaster.listStatus(parentPath, ListStatusContext.defaults());
    long newAccessTime = mFsMaster.getFileInfo(parentId).getLastAccessTimeMs();
    // time is changed in master
    Assert.assertNotEquals(newAccessTime, oldAccessTime);
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsm = masterResource.getRegistry().get(FileSystemMaster.class);
      long journaledAccessTime = fsm.getFileInfo(parentId).getLastAccessTimeMs();
      // time is not flushed to journal
      Assert.assertEquals(journaledAccessTime, oldAccessTime);
    }
    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsm = masterResource.getRegistry().get(FileSystemMaster.class);
      long journaledAccessTimeAfterStop = fsm.getFileInfo(parentId).getLastAccessTimeMs();
      // time is now flushed to journal
      Assert.assertEquals(journaledAccessTimeAfterStop, newAccessTime);
    }
  }

  /**
   * Tests journal is not updated with access time asynchronously after delete.
   */
  @Test
  public void updateAccessTimeAsyncFlushAfterDelete() throws Exception {
    String parentName = "d1";
    AlluxioURI parentPath = new AlluxioURI("/" + parentName);
    long parentId = mFsMaster.createDirectory(parentPath,
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)
            .setMode(new Mode((short) 0700).toProto())));
    long oldAccessTime = mFsMaster.getFileInfo(parentId).getLastAccessTimeMs();
    Thread.sleep(100);
    mFsMaster.listStatus(parentPath, ListStatusContext.defaults());
    long newAccessTime = mFsMaster.getFileInfo(parentId).getLastAccessTimeMs();
    // time is changed in master
    Assert.assertNotEquals(newAccessTime, oldAccessTime);
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsm = masterResource.getRegistry().get(FileSystemMaster.class);
      long journaledAccessTime = fsm.getFileInfo(parentId).getLastAccessTimeMs();
      // time is not flushed to journal
      Assert.assertEquals(journaledAccessTime, oldAccessTime);
      // delete the directory
      mFsMaster.delete(parentPath, DeleteContext.defaults());
    }
    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsm = masterResource.getRegistry().get(FileSystemMaster.class);
      Assert.assertEquals(fsm.getFileId(parentPath), -1);
    }
  }

  /**
   * This class provides multiple concurrent threads to create all files in one directory.
   */
  class ConcurrentCreator implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private AlluxioURI mInitPath;
    private CreateFileContext mCreateFileContext;

    /**
     * Constructs the concurrent creator.
     *
     * @param depth the depth of files to be created in one directory
     * @param concurrencyDepth the concurrency depth of files to be created in one directory
     * @param initPath the directory of files to be created in
     */
    ConcurrentCreator(int depth, int concurrencyDepth, AlluxioURI initPath) {
      this(depth, concurrencyDepth, initPath, CreateFileContext.defaults());
    }

    /**
     * Constructs the concurrent creator.
     *
     * @param depth the depth of files to be created in one directory
     * @param concurrencyDepth the concurrency depth of files to be created in one directory
     * @param initPath the directory of files to be created in
     * @param context method context
     */
    ConcurrentCreator(int depth, int concurrencyDepth, AlluxioURI initPath,
        CreateFileContext context) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mInitPath = initPath;
      mCreateFileContext = context;
    }

    /**
     * Authenticates the client user named TEST_USER and executes the process of creating all
     * files in one directory by multiple concurrent threads.
     *
     * @return null
     */
    @Override
    @Nullable
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    /**
     * Executes the process of creating all files in one directory by multiple concurrent threads.
     *
     * @param depth the depth of files to be created in one directory
     * @param concurrencyDepth the concurrency depth of files to be created in one directory
     * @param path the directory of files to be created in
     */
    public void exec(int depth, int concurrencyDepth, AlluxioURI path) throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1) {
        long fileId = mFsMaster.createFile(path, mCreateFileContext).getFileId();
        Assert.assertEquals(fileId, mFsMaster.getFileId(path));
        // verify the user permission for file
        FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
        Assert.assertEquals(TEST_USER, fileInfo.getOwner());
        Assert.assertEquals(0644, (short) fileInfo.getMode());
      } else {
        mFsMaster.createDirectory(path, CreateDirectoryContext.defaults());
        Assert.assertNotNull(mFsMaster.getFileId(path));
        long dirId = mFsMaster.getFileId(path);
        Assert.assertNotEquals(-1, dirId);
        FileInfo dirInfo = mFsMaster.getFileInfo(dirId);
        Assert.assertEquals(TEST_USER, dirInfo.getOwner());
        Assert.assertEquals(0755, (short) dirInfo.getMode());
      }

      if (concurrencyDepth > 0) {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
          ArrayList<Future<Void>> futures = new ArrayList<>(FILES_PER_NODE);
          for (int i = 0; i < FILES_PER_NODE; i++) {
            Callable<Void> call = (new ConcurrentCreator(depth - 1, concurrencyDepth - 1,
                path.join(Integer.toString(i)), mCreateFileContext));
            futures.add(executor.submit(call));
          }
          for (Future<Void> f : futures) {
            f.get();
          }
        } finally {
          executor.shutdown();
        }
      } else {
        for (int i = 0; i < FILES_PER_NODE; i++) {
          exec(depth - 1, concurrencyDepth, path.join(Integer.toString(i)));
        }
      }
    }
  }

  /**
   * This class provides multiple concurrent threads to free all files in one directory.
   */
  class ConcurrentFreer implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private AlluxioURI mInitPath;

    /**
     * Constructs the concurrent freer.
     *
     * @param depth the depth of files to be freed
     * @param concurrencyDepth the concurrency depth of files to be freed
     * @param initPath the directory of files to be freed
     */
    ConcurrentFreer(int depth, int concurrencyDepth, AlluxioURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mInitPath = initPath;
    }

    @Override
    @Nullable
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    private void doFree(AlluxioURI path) throws Exception {
      mFsMaster.free(path,
          FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(true).setRecursive(true)));
      Assert.assertNotEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(path));
    }

    /**
     * Executes the process of freeing all files in one directory by multiple concurrent threads.
     *
     * @param depth the depth of files to be freed in one directory
     * @param concurrencyDepth the concurrency depth of files to be freed in one directory
     * @param path the directory of files to be freed in
     */
    public void exec(int depth, int concurrencyDepth, AlluxioURI path) throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1 || (path.hashCode() % 10 == 0)) {
        // Sometimes we want to try freeing a path when we're not all the way down, which is what
        // the second condition is for.
        doFree(path);
      } else {
        if (concurrencyDepth > 0) {
          ExecutorService executor = Executors.newCachedThreadPool();
          try {
            ArrayList<Future<Void>> futures = new ArrayList<>(FILES_PER_NODE);
            for (int i = 0; i < FILES_PER_NODE; i++) {
              Callable<Void> call = (new ConcurrentDeleter(depth - 1, concurrencyDepth - 1,
                  path.join(Integer.toString(i))));
              futures.add(executor.submit(call));
            }
            for (Future<Void> f : futures) {
              f.get();
            }
          } finally {
            executor.shutdown();
          }
        } else {
          for (int i = 0; i < FILES_PER_NODE; i++) {
            exec(depth - 1, concurrencyDepth, path.join(Integer.toString(i)));
          }
        }
        doFree(path);
      }
    }
  }

  /**
   * This class provides multiple concurrent threads to delete all files in one directory.
   */
  class ConcurrentDeleter implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private AlluxioURI mInitPath;

    /**
     * Constructs the concurrent deleter.
     *
     * @param depth the depth of files to be deleted in one directory
     * @param concurrencyDepth the concurrency depth of files to be deleted in one directory
     * @param initPath the directory of files to be deleted in
     */
    ConcurrentDeleter(int depth, int concurrencyDepth, AlluxioURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mInitPath = initPath;
    }

    @Override
    @Nullable
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    private void doDelete(AlluxioURI path) throws Exception {
      mFsMaster.delete(path,
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(path));
    }

    /**
     * Executes the process of deleting all files in one directory by multiple concurrent threads.
     *
     * @param depth the depth of files to be deleted in one directory
     * @param concurrencyDepth the concurrency depth of files to be deleted in one directory
     * @param path the directory of files to be deleted in
     */
    public void exec(int depth, int concurrencyDepth, AlluxioURI path) throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1 || (path.hashCode() % 10 == 0)) {
        // Sometimes we want to try deleting a path when we're not all the way down, which is what
        // the second condition is for.
        doDelete(path);
      } else {
        if (concurrencyDepth > 0) {
          ExecutorService executor = Executors.newCachedThreadPool();
          try {
            ArrayList<Future<Void>> futures = new ArrayList<>(FILES_PER_NODE);
            for (int i = 0; i < FILES_PER_NODE; i++) {
              Callable<Void> call = (new ConcurrentDeleter(depth - 1, concurrencyDepth - 1,
                  path.join(Integer.toString(i))));
              futures.add(executor.submit(call));
            }
            for (Future<Void> f : futures) {
              f.get();
            }
          } finally {
            executor.shutdown();
          }
        } else {
          for (int i = 0; i < FILES_PER_NODE; i++) {
            exec(depth - 1, concurrencyDepth, path.join(Integer.toString(i)));
          }
        }
        doDelete(path);
      }
    }
  }

  /**
   * This class runs multiple concurrent threads to rename all files in one directory.
   */
  class ConcurrentRenamer implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private AlluxioURI mRootPath;
    private AlluxioURI mRootPath2;
    private AlluxioURI mInitPath;

    /**
     * Constructs the concurrent renamer.
     *
     * @param depth the depth of files to be renamed in one directory
     * @param concurrencyDepth the concurrency depth of files to be renamed in one directory
     * @param rootPath  the source root path of the files to be renamed
     * @param rootPath2 the destination root path of the files to be renamed
     * @param initPath the directory of files to be renamed in under root path
     */
    ConcurrentRenamer(int depth, int concurrencyDepth, AlluxioURI rootPath, AlluxioURI rootPath2,
        AlluxioURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mRootPath = rootPath;
      mRootPath2 = rootPath2;
      mInitPath = initPath;
    }

    @Override
    @Nullable
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    /**
     * Renames all files in one directory using multiple concurrent threads.
     *
     * @param depth the depth of files to be renamed in one directory
     * @param concurrencyDepth the concurrency depth of files to be renamed in one directory
     * @param path the directory of files to be renamed in under root path
     */
    public void exec(int depth, int concurrencyDepth, AlluxioURI path) throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1 || (depth < mDepth && path.hashCode() % 10 < 3)) {
        // Sometimes we want to try renaming a path when we're not all the way down, which is what
        // the second condition is for. We have to create the path in the destination up till what
        // we're renaming. This might already exist, so createFile could throw a
        // FileAlreadyExistsException, which we silently handle.
        AlluxioURI srcPath = mRootPath.join(path);
        AlluxioURI dstPath = mRootPath2.join(path);
        long fileId = mFsMaster.getFileId(srcPath);
        try {
          CreateDirectoryContext context = CreateDirectoryContext
              .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true));
          mFsMaster.createDirectory(dstPath.getParent(), context);
        } catch (FileAlreadyExistsException | InvalidPathException e) {
          // FileAlreadyExistsException: This is an acceptable exception to get, since we
          // don't know if the parent has been created yet by another thread.
          // InvalidPathException: This could happen if we are renaming something that's a child of
          // the root.
        }
        mFsMaster.rename(srcPath, dstPath, RenameContext.defaults());
        Assert.assertEquals(fileId, mFsMaster.getFileId(dstPath));
      } else if (concurrencyDepth > 0) {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
          ArrayList<Future<Void>> futures = new ArrayList<>(FILES_PER_NODE);
          for (int i = 0; i < FILES_PER_NODE; i++) {
            Callable<Void> call = (new ConcurrentRenamer(depth - 1, concurrencyDepth - 1, mRootPath,
                mRootPath2, path.join(Integer.toString(i))));
            futures.add(executor.submit(call));
          }
          for (Future<Void> f : futures) {
            f.get();
          }
        } finally {
          executor.shutdown();
        }
      } else {
        for (int i = 0; i < FILES_PER_NODE; i++) {
          exec(depth - 1, concurrencyDepth, path.join(Integer.toString(i)));
        }
      }
    }
  }

  /**
   * A class to start a thread that creates a file, completes the file and then deletes the file.
   */
  private class ConcurrentCreateDelete implements Callable<Void> {
    private final CyclicBarrier mStartBarrier;
    private final AtomicBoolean mStopThread;
    private final AlluxioURI[] mFiles;

    /**
     * Concurrent create and delete of file.
     * @param barrier cyclic barrier
     * @param stopThread stop Thread
     * @param files files to create or delete
     */
    public ConcurrentCreateDelete(CyclicBarrier barrier, AtomicBoolean stopThread,
        AlluxioURI[] files) {
      mStartBarrier = barrier;
      mStopThread = stopThread;
      mFiles = files;
    }

    @Override
    @Nullable
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      mStartBarrier.await();
      Random random = new Random();
      while (!mStopThread.get()) {
        int id = random.nextInt(mFiles.length);
        try {
          // Create and complete a random file.
          mFsMaster.createFile(mFiles[id], CreateFileContext.defaults());
          mFsMaster.completeFile(mFiles[id], CompleteFileContext.defaults());
        } catch (FileAlreadyExistsException | FileDoesNotExistException
            | FileAlreadyCompletedException | InvalidPathException e) {
          // Ignore
        } catch (Exception e) {
          throw e;
        }
        id = random.nextInt(mFiles.length);
        try {
          // Delete a random file.
          mFsMaster.delete(mFiles[id], DeleteContext.defaults());
        } catch (FileDoesNotExistException | InvalidPathException e) {
          // Ignore
        } catch (Exception e) {
          throw e;
        }
      }
      return null;
    }
  }
}
