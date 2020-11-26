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
import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.file.FileSystemMaster;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemFactory;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemOptions;
import alluxio.util.CommonUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

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
public class ConcurrentRenameIntegrationTest extends BaseIntegrationTest {
  private static final String TEST_USER = "test";
  private static final int CONCURRENCY_FACTOR = 50;
  /** Duration to sleep during the rename call to show the benefits of concurrency. */
  private static final long SLEEP_MS = Constants.SECOND_MS / 10;
  /** Timeout for the concurrent test after which we will mark the test as failed. */
  private static final long LIMIT_MS = SLEEP_MS * CONCURRENCY_FACTOR / 2;
  /**
   * Options to mark a created file as persisted. Note that this does not actually persist the
   * file but flag the file to be treated as persisted, which will invoke ufs operations.
   */
  private static CreateFilePOptions sCreatePersistedFileOptions =
      CreateFilePOptions.newBuilder().setWriteType(WritePType.THROUGH).build();
  private static CreateDirectoryPOptions sCreatePersistedDirOptions = CreateDirectoryPOptions
      .newBuilder().setWriteType(WritePType.THROUGH).setRecursive(true).build();

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
   * Tests concurrent renames within the root do not block on each other.
   */
  @Test
  public void rootConcurrentRename() throws Exception {
    final int numThreads = CONCURRENCY_FACTOR;
    AlluxioURI[] srcs = new AlluxioURI[numThreads];
    AlluxioURI[] dsts = new AlluxioURI[numThreads];

    for (int i = 0; i < numThreads; i++) {
      srcs[i] = new AlluxioURI("/file" + i);
      mFileSystem.createFile(srcs[i], sCreatePersistedFileOptions).close();
      dsts[i] = new AlluxioURI("/renamed" + i);
    }

    assertErrorsSizeEquals(concurrentRename(srcs, dsts), 0);

    List<URIStatus> files = mFileSystem.listStatus(new AlluxioURI("/"));
    Collections.sort(files, new ConcurrentFileSystemMasterUtils.IntegerSuffixedPathComparator());
    for (int i = 0; i < numThreads; i++) {
      Assert.assertEquals(dsts[i].getName(), files.get(i).getName());
    }
    Assert.assertEquals(numThreads, files.size());
  }

  /**
   * Tests concurrent renames within a folder do not block on each other.
   */
  @Test
  public void folderConcurrentRename() throws Exception {
    final int numThreads = CONCURRENCY_FACTOR;
    AlluxioURI[] srcs = new AlluxioURI[numThreads];
    AlluxioURI[] dsts = new AlluxioURI[numThreads];

    AlluxioURI dir = new AlluxioURI("/dir");

    mFileSystem.createDirectory(dir);

    for (int i = 0; i < numThreads; i++) {
      srcs[i] = dir.join("/file" + i);
      mFileSystem.createFile(srcs[i], sCreatePersistedFileOptions).close();
      dsts[i] = dir.join("/renamed" + i);
    }

    assertErrorsSizeEquals(concurrentRename(srcs, dsts), 0);

    List<URIStatus> files = mFileSystem.listStatus(new AlluxioURI("/dir"));
    Collections.sort(files, new ConcurrentFileSystemMasterUtils.IntegerSuffixedPathComparator());
    for (int i = 0; i < numThreads; i++) {
      Assert.assertEquals(dsts[i].getName(), files.get(i).getName());
    }
    Assert.assertEquals(numThreads, files.size());
  }

  /**
   * Tests that many threads concurrently renaming the same file will only succeed once.
   */
  @Test
  public void sameFileConcurrentRename() throws Exception {
    int numThreads = CONCURRENCY_FACTOR;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    for (int i = 0; i < numThreads; i++) {
      srcs[i] = new AlluxioURI("/file");
      dsts[i] = new AlluxioURI("/renamed" + i);
    }

    // Create the one source file
    mFileSystem.createFile(srcs[0], sCreatePersistedFileOptions).close();

    ConcurrentHashSet<Throwable> errors = concurrentRename(srcs, dsts);

    // We should get an error for all but 1 rename
    Assert.assertEquals(numThreads - 1, errors.size());

    List<URIStatus> files = mFileSystem.listStatus(new AlluxioURI("/"));

    // Only one renamed file should exist
    Assert.assertEquals(1, files.size());
    Assert.assertTrue(files.get(0).getName().startsWith("renamed"));
  }

  /**
   * Tests that many threads concurrently renaming the same directory will only succeed once.
   */
  @Test
  public void sameDirConcurrentRename() throws Exception {
    int numThreads = CONCURRENCY_FACTOR;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    for (int i = 0; i < numThreads; i++) {
      srcs[i] = new AlluxioURI("/dir");
      dsts[i] = new AlluxioURI("/renamed" + i);
    }

    // Create the one source directory
    mFileSystem.createDirectory(srcs[0]);
    mFileSystem.createFile(new AlluxioURI("/dir/file"), sCreatePersistedFileOptions).close();

    ConcurrentHashSet<Throwable> errors = concurrentRename(srcs, dsts);

    // We should get an error for all but 1 rename
    assertErrorsSizeEquals(errors, numThreads - 1);
    // Only one renamed dir should exist
    List<URIStatus> existingDirs = mFileSystem.listStatus(new AlluxioURI("/"));
    Assert.assertEquals(1, existingDirs.size());
    Assert.assertTrue(existingDirs.get(0).getName().startsWith("renamed"));
    // The directory should contain the file
    List<URIStatus> dirChildren =
        mFileSystem.listStatus(new AlluxioURI(existingDirs.get(0).getPath()));
    Assert.assertEquals(1, dirChildren.size());
  }

  /**
   * Tests renaming files concurrently to the same destination will only succeed once.
   */
  @Test
  public void sameDstConcurrentRename() throws Exception {
    int numThreads = CONCURRENCY_FACTOR;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    for (int i = 0; i < numThreads; i++) {
      srcs[i] = new AlluxioURI("/file" + i);
      mFileSystem.createFile(srcs[i], sCreatePersistedFileOptions).close();
      dsts[i] = new AlluxioURI("/renamed");
    }

    ConcurrentHashSet<Throwable> errors = concurrentRename(srcs, dsts);

    // We should get an error for all but 1 rename.
    assertErrorsSizeEquals(errors, numThreads - 1);

    List<URIStatus> files = mFileSystem.listStatus(new AlluxioURI("/"));
    // Store file names in a set to ensure the names are all unique.
    Set<String> renamedFiles = new HashSet<>();
    Set<String> originalFiles = new HashSet<>();
    for (URIStatus file : files) {
      if (file.getName().startsWith("renamed")) {
        renamedFiles.add(file.getName());
      }
      if (file.getName().startsWith("file")) {
        originalFiles.add(file.getName());
      }
    }
    // One renamed file should exist, and numThreads - 1 original source files
    Assert.assertEquals(numThreads, files.size());
    Assert.assertEquals(1, renamedFiles.size());
    Assert.assertEquals(numThreads - 1, originalFiles.size());
  }

  /**
   * Tests renaming files concurrently from one directory to another succeeds.
   */
  @Test
  public void twoDirConcurrentRename() throws Exception {
    int numThreads = CONCURRENCY_FACTOR;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    AlluxioURI dir1 = new AlluxioURI("/dir1");
    AlluxioURI dir2 = new AlluxioURI("/dir2");
    mFileSystem.createDirectory(dir1);
    mFileSystem.createDirectory(dir2);
    for (int i = 0; i < numThreads; i++) {
      srcs[i] = dir1.join("file" + i);
      mFileSystem.createFile(srcs[i], sCreatePersistedFileOptions).close();
      dsts[i] = dir2.join("renamed" + i);
    }

    ConcurrentHashSet<Throwable> errors = concurrentRename(srcs, dsts);

    // We should get no errors
    assertErrorsSizeEquals(errors, 0);

    List<URIStatus> dir1Files = mFileSystem.listStatus(dir1);
    List<URIStatus> dir2Files = mFileSystem.listStatus(dir2);

    Assert.assertEquals(0, dir1Files.size());
    Assert.assertEquals(numThreads, dir2Files.size());

    Collections
        .sort(dir2Files, new ConcurrentFileSystemMasterUtils.IntegerSuffixedPathComparator());
    for (int i = 0; i < numThreads; i++) {
      Assert.assertEquals(dsts[i].getName(), dir2Files.get(i).getName());
    }
  }

  /**
   * Tests renaming files concurrently from and to two directories succeeds.
   */
  @Test
  public void acrossDirConcurrentRename() throws Exception {
    int numThreads = CONCURRENCY_FACTOR;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    AlluxioURI dir1 = new AlluxioURI("/dir1");
    AlluxioURI dir2 = new AlluxioURI("/dir2");
    mFileSystem.createDirectory(dir1);
    mFileSystem.createDirectory(dir2);
    for (int i = 0; i < numThreads; i++) {
      // Dir1 has even files, dir2 has odd files.
      if (i % 2 == 0) {
        srcs[i] = dir1.join("file" + i);
        dsts[i] = dir2.join("renamed" + i);
      } else {
        srcs[i] = dir2.join("file" + i);
        dsts[i] = dir1.join("renamed" + i);
      }
      mFileSystem.createFile(srcs[i], sCreatePersistedFileOptions).close();
    }

    ConcurrentHashSet<Throwable> errors = concurrentRename(srcs, dsts);

    // We should get no errors.
    assertErrorsSizeEquals(errors, 0);

    List<URIStatus> dir1Files = mFileSystem.listStatus(dir1);
    List<URIStatus> dir2Files = mFileSystem.listStatus(dir2);

    Assert.assertEquals(numThreads / 2, dir1Files.size());
    Assert.assertEquals(numThreads / 2, dir2Files.size());

    Collections
        .sort(dir1Files, new ConcurrentFileSystemMasterUtils.IntegerSuffixedPathComparator());
    for (int i = 1; i < numThreads; i += 2) {
      Assert.assertEquals(dsts[i].getName(), dir1Files.get(i / 2).getName());
    }

    Collections
        .sort(dir2Files, new ConcurrentFileSystemMasterUtils.IntegerSuffixedPathComparator());
    for (int i = 0; i < numThreads; i += 2) {
      Assert.assertEquals(dsts[i].getName(), dir2Files.get(i / 2).getName());
    }
  }

  /**
   * Tests renaming files concurrently under directories with a shared path prefix.
   */
  @Test
  public void sharedPrefixDirConcurrentRename() throws Exception {
    int numThreads = CONCURRENCY_FACTOR;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    AlluxioURI dir1 = new AlluxioURI("/root/dir1");
    AlluxioURI dir2 = new AlluxioURI("/root/parent/dir2");
    AlluxioURI dst = new AlluxioURI("/dst");
    mFileSystem.createDirectory(dir1, sCreatePersistedDirOptions);
    mFileSystem.createDirectory(dir2, sCreatePersistedDirOptions);
    mFileSystem.createDirectory(dst, sCreatePersistedDirOptions);
    for (int i = 0; i < numThreads; i++) {
      // Dir1 has even files, dir2 has odd files.
      srcs[i] = i % 2 == 0 ? dir1.join("file" + i) : dir2.join("file" + i);
      dsts[i] = dst.join("renamed" + i);
      mFileSystem.createFile(srcs[i], sCreatePersistedFileOptions).close();
    }

    ConcurrentHashSet<Throwable> errors = concurrentRename(srcs, dsts);

    // We should get no errors.
    assertErrorsSizeEquals(errors, 0);

    List<URIStatus> dir1Files = mFileSystem.listStatus(dir1);
    List<URIStatus> dir2Files = mFileSystem.listStatus(dir2);
    List<URIStatus> dstFiles = mFileSystem.listStatus(dst);

    Assert.assertEquals(0, dir1Files.size());
    Assert.assertEquals(0, dir2Files.size());
    Assert.assertEquals(numThreads, dstFiles.size());

    Collections.sort(dstFiles, new ConcurrentFileSystemMasterUtils.IntegerSuffixedPathComparator());
    for (int i = 0; i < numThreads; i++) {
      Assert.assertEquals(dsts[i].getName(), dstFiles.get(i).getName());
    }
  }

  /**
   * Tests renaming files concurrently under non persisted directories with a shared path prefix.
   */
  @Test
  public void sharedPrefixDirConcurrentRenameNonPersisted() throws Exception {
    int numThreads = CONCURRENCY_FACTOR;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    AlluxioURI dir1 = new AlluxioURI("/root/dir1");
    AlluxioURI dir2 = new AlluxioURI("/root/parent/dir2");
    AlluxioURI dst = new AlluxioURI("/dst");
    mFileSystem.createDirectory(dir1,
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    mFileSystem.createDirectory(dir2,
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    mFileSystem.createDirectory(dst,
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    for (int i = 0; i < numThreads; i++) {
      // Dir1 has even files, dir2 has odd files.
      srcs[i] = i % 2 == 0 ? dir1.join("file" + i) : dir2.join("file" + i);
      dsts[i] = dst.join("renamed" + i);
      mFileSystem.createFile(srcs[i], sCreatePersistedFileOptions).close();
    }

    ConcurrentHashSet<Throwable> errors = concurrentRename(srcs, dsts);

    // We should get no errors.
    assertErrorsSizeEquals(errors, 0);

    List<URIStatus> dir1Files = mFileSystem.listStatus(dir1);
    List<URIStatus> dir2Files = mFileSystem.listStatus(dir2);
    List<URIStatus> dstFiles = mFileSystem.listStatus(dst);

    Assert.assertEquals(0, dir1Files.size());
    Assert.assertEquals(0, dir2Files.size());
    Assert.assertEquals(numThreads, dstFiles.size());

    Collections.sort(dstFiles, new ConcurrentFileSystemMasterUtils.IntegerSuffixedPathComparator());
    for (int i = 0; i < numThreads; i++) {
      Assert.assertEquals(dsts[i].getName(), dstFiles.get(i).getName());
    }
  }

  private void assertErrorsSizeEquals(ConcurrentHashSet<Throwable> errors, int expected) {
    if (errors.size() != expected) {
      Assert.fail(String.format("Expected %d errors, but got %d, errors:\n",
          expected, errors.size()) + Joiner.on("\n").join(errors));
    }
  }

  /**
   * Helper for renaming a list of paths concurrently. Assumes the srcs are already created and
   * dsts do not exist. Enforces that the run time of this method is not greater than twice the
   * sleep time (to infer concurrent operations). Injects an artificial sleep time to the
   * sleeping under file system and resets it after the renames are complete.
   *
   * @param src list of source paths
   * @param dst list of destination paths
   * @return the occurred errors
   */
  private ConcurrentHashSet<Throwable> concurrentRename(
      final AlluxioURI[] src, final AlluxioURI[] dst) throws Exception {
    final int numFiles = src.length;
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
            mFileSystem.rename(src[iteration], dst[iteration]);
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
    Assert.assertTrue("Execution duration " + durationMs + " took longer than expected " + LIMIT_MS,
        durationMs < LIMIT_MS);
    return errors;
  }

  /**
   * Tests that getFileInfo (read operation) either returns the correct file info or fails if it
   * has been renamed while the operation was waiting for the file lock.
   */
  @Test
  public void consistentGetFileInfo() throws Exception {
    final int iterations = CONCURRENCY_FACTOR;
    final AlluxioURI file = new AlluxioURI("/file");
    final AlluxioURI dst = new AlluxioURI("/dst");
    final CyclicBarrier barrier = new CyclicBarrier(2);
    // If there are exceptions, we will store them here.
    final ConcurrentHashSet<Throwable> errors = new ConcurrentHashSet<>();
    Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        errors.add(ex);
      }
    };
    for (int i = 0; i < iterations; i++) {
      // Don't want sleeping ufs behavior, so do not write to ufs
      mFileSystem
          .createFile(file,
              CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build())
          .close();
      Thread renamer = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            AuthenticatedClientUser.set(TEST_USER);
            barrier.await();
            mFileSystem.rename(file, dst);
            mFileSystem.delete(dst);
          } catch (Exception e) {
            Assert.fail(e.getMessage());
          }
        }
      });
      renamer.setUncaughtExceptionHandler(exceptionHandler);
      Thread reader = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            AuthenticatedClientUser.set(TEST_USER);
            barrier.await();
            URIStatus status = mFileSystem.getStatus(file);
            // If the uri status is successfully obtained, then the path should match
            Assert.assertEquals(file.getName(), status.getName());
          } catch (AlluxioException e) {
            // AlluxioException - if the file is renamed while the thread waits for the lock.
            // FileDoesNotExistException - if the file is fully renamed before the getFileInfo call.
          } catch (Exception e) {
            Assert.fail(e.getMessage());
          }
        }
      });
      reader.setUncaughtExceptionHandler(exceptionHandler);
      renamer.start();
      reader.start();
      renamer.join();
      reader.join();
      Assert.assertTrue("Errors detected: " + errors, errors.isEmpty());
    }
  }
}
