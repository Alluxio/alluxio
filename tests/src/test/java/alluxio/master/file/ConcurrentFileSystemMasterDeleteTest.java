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

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.BaseIntegrationTest;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.sleepfs.SleepingUnderFileSystemFactory;
import alluxio.underfs.sleepfs.SleepingUnderFileSystemOptions;

import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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
public class ConcurrentFileSystemMasterDeleteTest extends BaseIntegrationTest {
  private static final String TEST_USER = "test";
  private static final int CONCURRENCY_FACTOR = 50;
  /** Duration to sleep during the rename call to show the benefits of concurrency. */
  private static final long SLEEP_MS = Constants.SECOND_MS;
  /** Timeout for the concurrent test after which we will mark the test as failed. */
  private static final long LIMIT_MS = SLEEP_MS * CONCURRENCY_FACTOR / 10;
  /**
   * Options to mark a created file as persisted. Note that this does not actually persist the
   * file but flag the file to be treated as persisted, which will invoke ufs operations.
   */
  private static CreateFileOptions sCreatePersistedFileOptions =
      CreateFileOptions.defaults().setWriteType(WriteType.THROUGH);
  private static CreateDirectoryOptions sCreatePersistedDirOptions =
      CreateDirectoryOptions.defaults().setWriteType(WriteType.THROUGH);

  private static SleepingUnderFileSystemFactory sSleepingUfsFactory;

  private FileSystem mFileSystem;

  private String mLocalUfsPath = Files.createTempDir().getAbsolutePath();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
          "sleep://" + mLocalUfsPath).setProperty(PropertyKey
          .USER_FILE_MASTER_CLIENT_THREADS, CONCURRENCY_FACTOR).build();

  // Must be done in beforeClass so execution is before rules
  @BeforeClass
  public static void beforeClass() throws Exception {
    SleepingUnderFileSystemOptions options = new SleepingUnderFileSystemOptions();
    sSleepingUfsFactory = new SleepingUnderFileSystemFactory(options);
    options.setDeleteFileMs(SLEEP_MS).setDeleteDirectoryMs(SLEEP_MS);
    UnderFileSystemFactoryRegistry.register(sSleepingUfsFactory);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UnderFileSystemFactoryRegistry.unregister(sSleepingUfsFactory);
  }

  @Before
  public void before() {
    mFileSystem = FileSystem.Factory.get();
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

    int errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.DELETE, paths,
            LIMIT_MS);

    Assert.assertEquals("More than 0 errors: " + errors, 0, errors);
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
    int errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.DELETE, paths,
            LIMIT_MS);

    Assert.assertEquals("More than 0 errors: " + errors, 0, errors);
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
    int errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.DELETE, paths,
            LIMIT_MS);

    Assert.assertEquals("More than 0 errors: " + errors, 0, errors);
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

    int errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.DELETE, paths,
            LIMIT_MS);

    // We should get an error for all but 1 delete
    Assert.assertEquals(numThreads - 1, errors);

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

    int errors = ConcurrentFileSystemMasterUtils
        .unaryOperation(mFileSystem, ConcurrentFileSystemMasterUtils.UnaryOperation.DELETE, paths,
            LIMIT_MS);

    // We should get an error for all but 1 delete
    Assert.assertEquals(numThreads - 1, errors);
    List<URIStatus> dirs = mFileSystem.listStatus(new AlluxioURI("/"));
    Assert.assertEquals(0, dirs.size());
  }
}
