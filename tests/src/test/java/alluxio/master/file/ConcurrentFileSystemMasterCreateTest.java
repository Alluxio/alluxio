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
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.collections.ConcurrentHashSet;
import alluxio.master.file.meta.PersistenceState;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.underfs.UnderFileSystemRegistry;
import alluxio.underfs.sleepfs.SleepingUnderFileSystemFactory;
import alluxio.underfs.sleepfs.SleepingUnderFileSystemOptions;
import alluxio.util.CommonUtils;
import alluxio.wire.LoadMetadataType;

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class ConcurrentFileSystemMasterCreateTest {
  private static final String TEST_USER = "test";
  private static final int CONCURRENCY_FACTOR = 50;
  /** Duration to sleep during the rename call to show the benefits of concurrency. */
  private static final long SLEEP_MS = Constants.SECOND_MS;
  /**
   * Options to mark a created file as persisted. Note that this does not actually persist the
   * file but flag the file to be treated as persisted, which will invoke ufs operations.
   */
  private static CreateFileOptions sCreatePersistedFileOptions =
      CreateFileOptions.defaults().setWriteType(WriteType.THROUGH);

  private static SleepingUnderFileSystemFactory sSleepingUfsFactory;

  private FileSystem mFileSystem;

  private String mLocalUfsPath = Files.createTempDir().getAbsolutePath();

  private enum UnaryOperation {
    CREATE,
    DELETE,
    GET_FILE_INFO,
    LIST_STATUS
  }

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().setProperty(PropertyKey.UNDERFS_ADDRESS,
          "sleep://" + mLocalUfsPath).setProperty(PropertyKey
          .USER_FILE_MASTER_CLIENT_THREADS, CONCURRENCY_FACTOR).build();

  // Must be done in beforeClass so execution is before rules
  @BeforeClass
  public static void beforeClass() throws Exception {
    SleepingUnderFileSystemOptions options = new SleepingUnderFileSystemOptions();
    sSleepingUfsFactory = new SleepingUnderFileSystemFactory(options);
    options.setMkdirsMs(SLEEP_MS).setIsDirectoryMs(SLEEP_MS);
    UnderFileSystemRegistry.register(sSleepingUfsFactory);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UnderFileSystemRegistry.unregister(sSleepingUfsFactory);
  }

  @Before
  public void before() {
    mFileSystem = FileSystem.Factory.get();
  }

  /**
   * Uses the integer suffix of a path to determine order. Paths without integer suffixes will be
   * ordered last.
   */
  private class IntegerSuffixedPathComparator implements Comparator<URIStatus> {
    @Override
    public int compare(URIStatus o1, URIStatus o2) {
      return extractIntegerSuffix(o1.getName()) - extractIntegerSuffix(o2.getName());
    }

    private int extractIntegerSuffix(String name) {
      Pattern p = Pattern.compile("\\D*(\\d+$)");
      Matcher m = p.matcher(name);
      if (m.matches()) {
        return Integer.parseInt(m.group(1));
      } else {
        return Integer.MAX_VALUE;
      }
    }
  }

  @Test
  public void concurrentCreate() throws Exception {
    final int numThreads = CONCURRENCY_FACTOR;
    // 7 nested components to create (2 seconds each).
    final long limitMs = 14 * SLEEP_MS * CONCURRENCY_FACTOR / 10;
    AlluxioURI[] paths = new AlluxioURI[numThreads];

    for (int i = 0; i < numThreads; i++) {
      paths[i] =
          new AlluxioURI("/existing/path/dir/shared_dir/t_" + i + "/sub_dir1/sub_dir2/file" + i);
    }
    int errors = concurrentUnaryOperation(UnaryOperation.CREATE, paths, limitMs);
    Assert.assertEquals("More than 0 errors: " + errors, 0, errors);
  }

  @Test
  public void concurrentCreateExistingDir() throws Exception {
    final int numThreads = CONCURRENCY_FACTOR;
    // 7 nested components to create (2 seconds each).
    final long limitMs = 14 * SLEEP_MS * CONCURRENCY_FACTOR / 10;
    AlluxioURI[] paths = new AlluxioURI[numThreads];

    // Create the existing path with MUST_CACHE, so subsequent creates have to persist the dirs.
    mFileSystem.createDirectory(new AlluxioURI("/existing/path/dir/"),
        CreateDirectoryOptions.defaults().setRecursive(true).setWriteType(WriteType.CACHE_THROUGH));

    for (int i = 0; i < numThreads; i++) {
      paths[i] =
          new AlluxioURI("/existing/path/dir/shared_dir/t_" + i + "/sub_dir1/sub_dir2/file" + i);
    }
    int errors = concurrentUnaryOperation(UnaryOperation.CREATE, paths, limitMs);
    Assert.assertEquals("More than 0 errors: " + errors, 0, errors);
  }

  @Test
  public void concurrentCreateNonPersistedDir() throws Exception {
    final int numThreads = CONCURRENCY_FACTOR;
    // 7 nested components to create (2 seconds each).
    final long limitMs = 14 * SLEEP_MS * CONCURRENCY_FACTOR / 10;
    AlluxioURI[] paths = new AlluxioURI[numThreads];

    // Create the existing path with MUST_CACHE, so subsequent creates have to persist the dirs.
    mFileSystem.createDirectory(new AlluxioURI("/existing/path/dir/"),
        CreateDirectoryOptions.defaults().setRecursive(true).setWriteType(WriteType.MUST_CACHE));

    for (int i = 0; i < numThreads; i++) {
      paths[i] =
          new AlluxioURI("/existing/path/dir/shared_dir/t_" + i + "/sub_dir1/sub_dir2/file" + i);
    }
    int errors = concurrentUnaryOperation(UnaryOperation.CREATE, paths, limitMs);
    Assert.assertEquals("More than 0 errors: " + errors, 0, errors);
  }

  @Test
  public void concurrentLoadFileMetadata() throws Exception {
    runLoadMetadata(null, false, true, false);
  }

  @Test
  public void concurrentLoadFileMetadataExistingDir() throws Exception {
    runLoadMetadata(WriteType.CACHE_THROUGH, false, true, false);
  }

  @Test
  public void concurrentLoadFileMetadataNonPersistedDir() throws Exception {
    runLoadMetadata(WriteType.MUST_CACHE, false, true, false);
  }

  @Test
  public void concurrentLoadSameFileMetadata() throws Exception {
    runLoadMetadata(null, true, true, false);
  }

  @Test
  public void concurrentLoadSameFileMetadataExistingDir() throws Exception {
    runLoadMetadata(WriteType.CACHE_THROUGH, true, true, false);
  }

  @Test
  public void concurrentLoadSameFileMetadataNonPersistedDir() throws Exception {
    runLoadMetadata(WriteType.MUST_CACHE, true, true, false);
  }

  @Test
  public void concurrentLoadDirMetadata() throws Exception {
    runLoadMetadata(null, false, false, false);
  }

  @Test
  public void concurrentLoadDirMetadataExistingDir() throws Exception {
    runLoadMetadata(WriteType.CACHE_THROUGH, false, false, false);
  }

  @Test
  public void concurrentLoadDirMetadataNonPersistedDir() throws Exception {
    runLoadMetadata(WriteType.MUST_CACHE, false, false, false);
  }

  @Test
  public void concurrentLoadSameDirMetadata() throws Exception {
    runLoadMetadata(null, true, false, false);
  }

  @Test
  public void concurrentLoadSameDirMetadataExistingDir() throws Exception {
    runLoadMetadata(WriteType.CACHE_THROUGH, true, false, false);
  }

  @Test
  public void concurrentLoadSameDirMetadataNonPersistedDir() throws Exception {
    runLoadMetadata(WriteType.MUST_CACHE, true, false, false);
  }

  @Test
  public void concurrentListDirs() throws Exception {
    runLoadMetadata(null, false, false, true);
  }

  @Test
  public void concurrentListDirsExistingDir() throws Exception {
    runLoadMetadata(WriteType.CACHE_THROUGH, false, false, true);
  }

  @Test
  public void concurrentListDirsNonPersistedDir() throws Exception {
    runLoadMetadata(WriteType.MUST_CACHE, false, false, true);
  }

  @Test
  public void concurrentListFiles() throws Exception {
    runLoadMetadata(null, false, true, true);
  }

  @Test
  public void concurrentListFilesExistingDir() throws Exception {
    runLoadMetadata(WriteType.CACHE_THROUGH, false, true, true);
  }

  @Test
  public void concurrentListFilesNonPersistedDir() throws Exception {
    runLoadMetadata(WriteType.MUST_CACHE, false, true, true);
  }

  private int concurrentUnaryOperation(final UnaryOperation operation, final AlluxioURI[] paths,
      final long limitMs) throws Exception {
    final int numFiles = paths.length;
    final CyclicBarrier barrier = new CyclicBarrier(numFiles);
    List<Thread> threads = new ArrayList<>(numFiles);
    // If there are exceptions, we will store them here.
    final ConcurrentHashSet<Throwable> errors = new ConcurrentHashSet<>();
    Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        errors.add(ex);
      }
    };
    for (int i = 0; i < numFiles; i++) {
      final int iteration = i;
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            AuthenticatedClientUser.set(TEST_USER);
            barrier.await();
            switch (operation) {
              case CREATE:
                mFileSystem.createFile(paths[iteration], sCreatePersistedFileOptions).close();
                break;
              case DELETE:
                mFileSystem.delete(paths[iteration]);
                break;
              case GET_FILE_INFO:
                mFileSystem.getStatus(paths[iteration]);
                break;
              case LIST_STATUS:
                mFileSystem.listStatus(paths[iteration]);
                break;
              default: throw new IllegalArgumentException("'operation' is not a valid operation.");
            }

          } catch (Exception e) {
            Throwables.propagate(e);
          }
        }
      });
      t.setUncaughtExceptionHandler(exceptionHandler);
      threads.add(t);
    }
    Collections.shuffle(threads);
    long startMs = CommonUtils.getCurrentMs();
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    long durationMs = CommonUtils.getCurrentMs() - startMs;
    Assert.assertTrue("Execution duration " + durationMs + " took longer than expected " + limitMs,
        durationMs < limitMs);
    return errors.size();
  }

  /**
   * Runs load metadata tests.
   *
   * @param writeType the {@link WriteType} to create ancestors, if not null
   * @param useSinglePath if true, threads will only use a single path
   * @param createFiles if true, will create files at the bottom of the tree, directories otherwise
   * @param listParentDir if true, will list the parent dir to load the metadata
   * @throws Exception if an error occurs
   */
  private void runLoadMetadata(WriteType writeType, boolean useSinglePath, boolean createFiles,
      boolean listParentDir) throws Exception {
    int numThreads = CONCURRENCY_FACTOR;
    // 2 nested components to create.
    long limitMs = 2 * SLEEP_MS * 3;

    int uniquePaths = useSinglePath ? 1 : numThreads;

    if (listParentDir) {
      // Loading direct children needs to load each child, so reduce the branching factor.
      uniquePaths = 10;
      limitMs = (2 + uniquePaths) * SLEEP_MS * 2;
    }

    // Create UFS files outside of Alluxio.
    new File(mLocalUfsPath + "/existing/path/").mkdirs();
    for (int i = 0; i < uniquePaths; i++) {
      if (createFiles) {
        FileWriter fileWriter = new FileWriter(mLocalUfsPath + "/existing/path/last_" + i);
        fileWriter.write("test");
        fileWriter.close();
      } else {
        new File(mLocalUfsPath + "/existing/path/last_" + i).mkdirs();
      }
    }

    if (writeType != null) {
      // create inodes in Alluxio
      mFileSystem.createDirectory(new AlluxioURI("/existing/path/"),
          CreateDirectoryOptions.defaults().setRecursive(true).setWriteType(writeType));
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

    int errors = 0;
    if (listParentDir) {
      errors = concurrentUnaryOperation(UnaryOperation.LIST_STATUS, paths, limitMs);
    } else {
      errors = concurrentUnaryOperation(UnaryOperation.GET_FILE_INFO, paths, limitMs);
    }
    Assert.assertEquals("More than 0 errors: " + errors, 0, errors);

    ListStatusOptions listOptions = ListStatusOptions.defaults().setLoadMetadataType(
        LoadMetadataType.Never);

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
    Collections.sort(files, new IntegerSuffixedPathComparator());
    for (int i = 0; i < uniquePaths; i++) {
      Assert.assertEquals("last_" + i, files.get(i).getName());
      Assert.assertEquals(PersistenceState.PERSISTED,
          PersistenceState.valueOf(files.get(i).getPersistenceState()));
    }
  }
}
