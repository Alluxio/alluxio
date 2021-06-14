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

package alluxio.client.fs.concurrent;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.conf.ServerConfiguration;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.UnderFileSystemFactoryRegistryRule;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.WritePType;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.PersistenceState;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemFactory;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemOptions;

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.Collections;
import java.util.List;

/**
 * Tests to validate the concurrency in {@link FileSystemMaster}. These tests all use a local
 * path as the under storage system.
 *
 * The tests validate the correctness of concurrent operations, ie. no corrupted/partial state is
 * exposed, through a series of concurrent operations followed by verification of the final
 * state, or inspection of the in-progress state as the operations are carried out.
 *
 * The tests also validate that operations are concurrent by injecting a short sleep in the
 * critical code path. Tests will timeout if the critical section is performed serially.
 */
public class ConcurrentFileSystemMasterCreateIntegrationTest extends BaseIntegrationTest {
  private static final String TEST_USER = "test";
  private static final int CONCURRENCY_FACTOR = 50;
  /** Duration to sleep during the rename call to show the benefits of concurrency. */
  private static final long SLEEP_MS = Constants.SECOND_MS / 10;

  private FileSystem mFileSystem;

  private String mLocalUfsPath = Files.createTempDir().getAbsolutePath();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER,
      ServerConfiguration.global());

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, "sleep://" + mLocalUfsPath)
          .setProperty(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX, CONCURRENCY_FACTOR)
          .setProperty(PropertyKey.USER_BLOCK_MASTER_CLIENT_POOL_SIZE_MAX, CONCURRENCY_FACTOR)
          /**
           * This is to make sure master executor has enough thread to being with. Otherwise, delay
           * on master ForkJoinPool's internal thread count adjustment might take several seconds.
           * This can interfere with this test's timing expectations.
           */
          .setProperty(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE, CONCURRENCY_FACTOR)
          .build();

  @ClassRule
  public static UnderFileSystemFactoryRegistryRule sUnderfilesystemfactoryregistry =
      new UnderFileSystemFactoryRegistryRule(new SleepingUnderFileSystemFactory(
          new SleepingUnderFileSystemOptions().setMkdirsMs(SLEEP_MS).setIsDirectoryMs(SLEEP_MS)
              .setGetFileStatusMs(SLEEP_MS).setIsFileMs(SLEEP_MS)));

  @Before
  public void before() {
    mFileSystem = FileSystem.Factory.create(ServerConfiguration.global());
    ServerConfiguration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "2b");
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  /**
   * Tests concurrent create of files. Files are created under one shared directory but different
   * sub-directories.
   */
  @Test
  public void concurrentCreate() throws Exception {
    final int numThreads = CONCURRENCY_FACTOR;
    // 7 nested components to create (2 seconds each).
    final long limitMs = 14 * SLEEP_MS * CONCURRENCY_FACTOR / 2;
    AlluxioURI[] paths = new AlluxioURI[numThreads];

    for (int i = 0; i < numThreads; i++) {
      paths[i] =
          new AlluxioURI("/existing/path/dir/shared_dir/t_" + i + "/sub_dir1/sub_dir2/file" + i);
    }
    List<Throwable> errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.CREATE, paths,
            limitMs);
    if (!errors.isEmpty()) {
      Assert.fail("Encountered " + errors.size() + " errors, the first one is " + errors.get(0));
    }
  }

  /**
   * Test concurrent create of existing directory. Existing directory is created as CACHE_THROUGH
   * then files are created under that directory.
   */
  @Test
  public void concurrentCreateExistingDir() throws Exception {
    final int numThreads = CONCURRENCY_FACTOR;
    // 7 nested components to create (2 seconds each).
    final long limitMs = 14 * SLEEP_MS * CONCURRENCY_FACTOR / 2;
    AlluxioURI[] paths = new AlluxioURI[numThreads];

    // Create the existing path with CACHE_THROUGH that it will be persisted.
    mFileSystem.createDirectory(new AlluxioURI("/existing/path/dir/"), CreateDirectoryPOptions
        .newBuilder().setRecursive(true).setWriteType(WritePType.CACHE_THROUGH).build());

    for (int i = 0; i < numThreads; i++) {
      paths[i] =
          new AlluxioURI("/existing/path/dir/shared_dir/t_" + i + "/sub_dir1/sub_dir2/file" + i);
    }
    List<Throwable> errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.CREATE, paths,
            limitMs);
    if (!errors.isEmpty()) {
      Assert.fail("Encountered " + errors.size() + " errors, the first one is " + errors.get(0));
    }
  }

  /**
   * Test concurrent create of non-persisted directory. Directory is created as MUST_CACHE then
   * files are created under that directory.
   */
  @Test
  public void concurrentCreateNonPersistedDir() throws Exception {
    final int numThreads = CONCURRENCY_FACTOR;
    // 7 nested components to create (2 seconds each).
    final long limitMs = 14 * SLEEP_MS * CONCURRENCY_FACTOR / 2;
    AlluxioURI[] paths = new AlluxioURI[numThreads];

    // Create the existing path with MUST_CACHE, so subsequent creates have to persist the dirs.
    mFileSystem.createDirectory(new AlluxioURI("/existing/path/dir/"), CreateDirectoryPOptions
        .newBuilder().setRecursive(true).setWriteType(WritePType.MUST_CACHE).build());

    for (int i = 0; i < numThreads; i++) {
      paths[i] =
          new AlluxioURI("/existing/path/dir/shared_dir/t_" + i + "/sub_dir1/sub_dir2/file" + i);
    }
    List<Throwable> errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.CREATE, paths,
            limitMs);
    if (!errors.isEmpty()) {
      Assert.fail("Encountered " + errors.size() + " errors, the first one is " + errors.get(0));
    }
  }

  @Test
  public void concurrentLoadFileMetadata() throws Exception {
    runLoadMetadata(null, false, true, false);
  }

  @Test
  public void concurrentLoadFileMetadataExistingDir() throws Exception {
    runLoadMetadata(WritePType.CACHE_THROUGH, false, true, false);
  }

  @Test
  public void concurrentLoadFileMetadataNonPersistedDir() throws Exception {
    runLoadMetadata(WritePType.MUST_CACHE, false, true, false);
  }

  @Test
  public void concurrentLoadSameFileMetadata() throws Exception {
    runLoadMetadata(null, true, true, false);
  }

  @Test
  public void concurrentLoadSameFileMetadataExistingDir() throws Exception {
    runLoadMetadata(WritePType.CACHE_THROUGH, true, true, false);
  }

  @Test
  public void concurrentLoadSameFileMetadataNonPersistedDir() throws Exception {
    runLoadMetadata(WritePType.MUST_CACHE, true, true, false);
  }

  @Test
  public void concurrentLoadDirMetadata() throws Exception {
    runLoadMetadata(null, false, false, false);
  }

  @Test
  public void concurrentLoadDirMetadataExistingDir() throws Exception {
    runLoadMetadata(WritePType.CACHE_THROUGH, false, false, false);
  }

  @Test
  public void concurrentLoadDirMetadataNonPersistedDir() throws Exception {
    runLoadMetadata(WritePType.MUST_CACHE, false, false, false);
  }

  @Test
  public void concurrentLoadSameDirMetadata() throws Exception {
    runLoadMetadata(null, true, false, false);
  }

  @Test
  public void concurrentLoadSameDirMetadataExistingDir() throws Exception {
    runLoadMetadata(WritePType.CACHE_THROUGH, true, false, false);
  }

  @Test
  public void concurrentLoadSameDirMetadataNonPersistedDir() throws Exception {
    runLoadMetadata(WritePType.MUST_CACHE, true, false, false);
  }

  @Test
  public void concurrentListDirs() throws Exception {
    runLoadMetadata(null, false, false, true);
  }

  @Test
  public void concurrentListDirsExistingDir() throws Exception {
    runLoadMetadata(WritePType.CACHE_THROUGH, false, false, true);
  }

  @Test
  public void concurrentListDirsNonPersistedDir() throws Exception {
    runLoadMetadata(WritePType.MUST_CACHE, false, false, true);
  }

  @Test
  public void concurrentListFiles() throws Exception {
    runLoadMetadata(null, false, true, true);
  }

  @Test
  public void concurrentListFilesExistingDir() throws Exception {
    runLoadMetadata(WritePType.CACHE_THROUGH, false, true, true);
  }

  @Test
  public void concurrentListFilesNonPersistedDir() throws Exception {
    runLoadMetadata(WritePType.MUST_CACHE, false, true, true);
  }

  /**
   * Runs load metadata tests.
   *
   * @param writeType the {@link WritePType} to create ancestors, if not null
   * @param useSinglePath if true, threads will only use a single path
   * @param createFiles if true, will create files at the bottom of the tree, directories otherwise
   * @param listParentDir if true, will list the parent dir to load the metadata
   */
  private void runLoadMetadata(WritePType writeType, boolean useSinglePath, boolean createFiles,
      boolean listParentDir) throws Exception {
    int numThreads = CONCURRENCY_FACTOR;
    int bufferFactor = CONCURRENCY_FACTOR / 2;
    // 2 nested components to create.
    long limitMs = 2 * SLEEP_MS * bufferFactor;

    int uniquePaths = useSinglePath ? 1 : numThreads;

    if (writeType != WritePType.CACHE_THROUGH) {
      // all 3 components must be synced to UFS.
      limitMs = 3 * SLEEP_MS * bufferFactor;
    }

    if (listParentDir) {
      // Loading direct children needs to load each child, so reduce the branching factor.
      uniquePaths = 10;
      limitMs = 2 + uniquePaths * SLEEP_MS * bufferFactor;
    }

    // Create UFS files outside of Alluxio.
    new File(mLocalUfsPath + "/existing/path/").mkdirs();
    for (int i = 0; i < uniquePaths; i++) {
      if (createFiles) {
        FileWriter fileWriter = new FileWriter(mLocalUfsPath + "/existing/path/last_" + i);
        fileWriter.write("testtesttesttest");
        fileWriter.close();
      } else {
        new File(mLocalUfsPath + "/existing/path/last_" + i).mkdirs();
      }
    }

    if (writeType != null) {
      // create inodes in Alluxio
      mFileSystem.createDirectory(new AlluxioURI("/existing/path/"),
          CreateDirectoryPOptions.newBuilder().setRecursive(true).setWriteType(writeType).build());
    }

    // Generate path names for threads.
    AlluxioURI[] paths = new AlluxioURI[numThreads];
    int fileId = 0;
    for (int i = 0; i < numThreads; i++) {
      if (listParentDir) {
        paths[i] = new AlluxioURI("/existing/path/");
      } else {
        paths[i] = new AlluxioURI("/existing/path/last_" + ((fileId++) % uniquePaths));
      }
    }

    List<Throwable> errors;
    if (listParentDir) {
      errors = ConcurrentFileSystemMasterUtils
          .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.LIST_STATUS,
              paths, limitMs);
    } else {
      errors = ConcurrentFileSystemMasterUtils
          .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.GET_FILE_INFO,
              paths, limitMs);
    }

    if (!errors.isEmpty()) {
      Assert.fail("Encountered " + errors.size() + " errors, the first one is " + errors.get(0)
          + "\n" + Throwables.getStackTraceAsString(errors.get(0)));
    }

    ListStatusPOptions listOptions =
        ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER).build();

    List<URIStatus> files = mFileSystem.listStatus(new AlluxioURI("/"), listOptions);
    Assert.assertEquals(1, files.size());
    Assert.assertEquals("existing", files.get(0).getName());
    Assert.assertEquals(PersistenceState.PERSISTED,
        PersistenceState.valueOf(files.get(0).getPersistenceState()));

    files = mFileSystem.listStatus(new AlluxioURI("/existing"), listOptions);
    Assert.assertEquals(1, files.size());
    Assert.assertEquals("path", files.get(0).getName());
    Assert.assertEquals(PersistenceState.PERSISTED,
        PersistenceState.valueOf(files.get(0).getPersistenceState()));

    files = mFileSystem.listStatus(new AlluxioURI("/existing/path/"), listOptions);
    Assert.assertEquals(uniquePaths, files.size());
    Collections.sort(files, new ConcurrentFileSystemMasterUtils.IntegerSuffixedPathComparator());
    for (int i = 0; i < uniquePaths; i++) {
      Assert.assertEquals("last_" + i, files.get(i).getName());
      Assert.assertEquals(PersistenceState.PERSISTED,
          PersistenceState.valueOf(files.get(i).getPersistenceState()));
    }
  }
}
