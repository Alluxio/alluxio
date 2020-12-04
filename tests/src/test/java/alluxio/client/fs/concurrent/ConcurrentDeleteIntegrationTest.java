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
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.UnderFileSystemFactoryRegistryRule;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.file.FileSystemMaster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemFactory;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemOptions;

import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

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
public class ConcurrentDeleteIntegrationTest extends BaseIntegrationTest {
  private static final String TEST_USER = "test";
  private static final int CONCURRENCY_FACTOR = 50;
  /** Duration to sleep during the rename call to show the benefits of concurrency. */
  private static final long SLEEP_MS = Constants.SECOND_MS;
  /** Timeout for the concurrent test after which we will mark the test as failed. */
  private static final long LIMIT_MS = SLEEP_MS * CONCURRENCY_FACTOR / 2;
  /**
   * Options to mark a created file as persisted. Note that this does not actually persist the
   * file but flag the file to be treated as persisted, which will invoke ufs operations.
   */
  private static CreateFilePOptions sCreatePersistedFileOptions =
      CreateFilePOptions.newBuilder().setWriteType(WritePType.THROUGH).build();
  private static CreateDirectoryPOptions sCreatePersistedDirOptions =
      CreateDirectoryPOptions.newBuilder().setWriteType(WritePType.THROUGH).build();

  private FileSystem mFileSystem;

  private String mLocalUfsPath = Files.createTempDir().getAbsolutePath();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER,
      ServerConfiguration.global());

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
          "sleep://" + mLocalUfsPath).setProperty(PropertyKey
          .USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX, CONCURRENCY_FACTOR).build();

  @ClassRule
  public static UnderFileSystemFactoryRegistryRule sUnderfilesystemfactoryregistry =
      new UnderFileSystemFactoryRegistryRule(new SleepingUnderFileSystemFactory(
          new SleepingUnderFileSystemOptions().setMkdirsMs(SLEEP_MS).setIsDirectoryMs(SLEEP_MS)));

  @Before
  public void before() {
    mFileSystem = FileSystem.Factory.create(ServerConfiguration.global());
  }

  /**
   * Tests concurrent deletes within the root do not block on each other.
   */
  @Test
  public void rootConcurrentDelete() throws Exception {
    final int numThreads = CONCURRENCY_FACTOR;
    AlluxioURI[] paths = new AlluxioURI[numThreads];

    for (int i = 0; i < numThreads; i++) {
      paths[i] = new AlluxioURI("/file" + i);
      mFileSystem.createFile(paths[i], sCreatePersistedFileOptions).close();
    }

    List<Throwable> errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.DELETE, paths,
            LIMIT_MS);
    if (!errors.isEmpty()) {
      Assert.fail("Encountered " + errors.size() + " errors, the first one is " + errors.get(0));
    }

    List<URIStatus> files = mFileSystem.listStatus(new AlluxioURI("/"));
    Assert.assertEquals(0, files.size());
  }

  /**
   * Tests concurrent deletes within a folder do not block on each other.
   */
  @Test
  public void folderConcurrentDelete() throws Exception {
    final int numThreads = CONCURRENCY_FACTOR;
    AlluxioURI[] paths = new AlluxioURI[numThreads];
    AlluxioURI dir = new AlluxioURI("/dir");
    mFileSystem.createDirectory(dir);

    for (int i = 0; i < numThreads; i++) {
      paths[i] = dir.join("/file" + i);
      mFileSystem.createFile(paths[i], sCreatePersistedFileOptions).close();
    }
    List<Throwable> errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.DELETE, paths,
            LIMIT_MS);
    if (!errors.isEmpty()) {
      Assert.fail("Encountered " + errors.size() + " errors, the first one is " + errors.get(0));
    }

    List<URIStatus> files = mFileSystem.listStatus(dir);
    Assert.assertEquals(0, files.size());
  }

  /**
   * Tests concurrent deletes with shared prefix do not block on each other.
   */
  @Test
  public void prefixConcurrentDelete() throws Exception {
    final int numThreads = CONCURRENCY_FACTOR;
    AlluxioURI[] paths = new AlluxioURI[numThreads];
    AlluxioURI dir1 = new AlluxioURI("/dir1");
    mFileSystem.createDirectory(dir1);
    AlluxioURI dir2 = new AlluxioURI("/dir1/dir2");
    mFileSystem.createDirectory(dir2);
    AlluxioURI dir3 = new AlluxioURI("/dir1/dir2/dir3");
    mFileSystem.createDirectory(dir3);

    for (int i = 0; i < numThreads; i++) {
      if (i % 3 == 0) {
        paths[i] = dir1.join("/file" + i);
      } else if (i % 3 == 1) {
        paths[i] = dir2.join("/file" + i);
      } else {
        paths[i] = dir3.join("/file" + i);
      }
      mFileSystem.createFile(paths[i], sCreatePersistedFileOptions).close();
    }
    List<Throwable> errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.DELETE, paths,
            LIMIT_MS);
    if (!errors.isEmpty()) {
      Assert.fail("Encountered " + errors.size() + " errors, the first one is " + errors.get(0));
    }
    List<URIStatus> files = mFileSystem.listStatus(dir1);
    // Should only contain a single directory
    Assert.assertEquals(1, files.size());
    Assert.assertEquals("dir2", files.get(0).getName());
    files = mFileSystem.listStatus(dir2);
    // Should only contain a single directory
    Assert.assertEquals(1, files.size());
    Assert.assertEquals("dir3", files.get(0).getName());
    files = mFileSystem.listStatus(dir3);
    Assert.assertEquals(0, files.size());
  }

  /**
   * Tests that many threads concurrently deleting the same file will only succeed once.
   */
  @Test
  public void sameFileConcurrentDelete() throws Exception {
    int numThreads = CONCURRENCY_FACTOR;
    final AlluxioURI[] paths = new AlluxioURI[numThreads];
    for (int i = 0; i < numThreads; i++) {
      paths[i] = new AlluxioURI("/file");
    }
    // Create the single file
    mFileSystem.createFile(paths[0], sCreatePersistedFileOptions).close();

    List<Throwable> errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.DELETE, paths,
            LIMIT_MS);

    // We should get an error for all but 1 delete
    Assert.assertEquals(numThreads - 1, errors.size());

    List<URIStatus> files = mFileSystem.listStatus(new AlluxioURI("/"));
    Assert.assertEquals(0, files.size());
  }

  /**
   * Tests that many threads concurrently deleting the same directory will only succeed once.
   */
  @Test
  public void sameDirConcurrentDelete() throws Exception {
    int numThreads = CONCURRENCY_FACTOR;
    final AlluxioURI[] paths = new AlluxioURI[numThreads];
    for (int i = 0; i < numThreads; i++) {
      paths[i] = new AlluxioURI("/dir");
    }
    // Create the single directory
    mFileSystem.createDirectory(paths[0], sCreatePersistedDirOptions);

    List<Throwable> errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.DELETE, paths,
            LIMIT_MS);

    // We should get an error for all but 1 delete
    Assert.assertEquals(numThreads - 1, errors.size());
    List<URIStatus> dirs = mFileSystem.listStatus(new AlluxioURI("/"));
    Assert.assertEquals(0, dirs.size());
  }
}
