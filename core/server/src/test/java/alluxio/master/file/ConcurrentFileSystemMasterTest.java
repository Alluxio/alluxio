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
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.journal.JournalFactory;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.local.LocalUnderFileSystem;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.FileInfo;

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
@RunWith(PowerMockRunner.class)
@PrepareForTest(UnderFileSystem.Factory.class)
public class ConcurrentFileSystemMasterTest {
  private static final String TEST_USER = "test";
  private static final long SLEEP_MS = Constants.SECOND_MS;
  /**
   * Options to mark a created file as persisted. Note that this does not actually persist the
   * file but flag the file to be treated as persisted, which will invoke ufs operations.
   */
  private static CreateFileOptions sCreatePersistedFileOptions;

  private BlockMaster mBlockMaster;
  private FileSystemMaster mFileSystemMaster;

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER);

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    LoginUserTestUtils.resetLoginUser();
    GroupMappingServiceTestUtils.resetCache();
    Configuration.set(PropertyKey.SECURITY_LOGIN_USERNAME, TEST_USER);
    // Set umask "000" to make default directory permission 0777 and default file permission 0666.
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "000");
    sCreatePersistedFileOptions = CreateFileOptions.defaults().setPersisted(true);

    // Cannot use multiple rules with Powermock, so use Java tmp dir
    String ufsAddress = Files.createTempDir().getAbsolutePath();
    String journalAddress = Files.createTempDir().getAbsolutePath();

    Configuration.set(PropertyKey.UNDERFS_ADDRESS, ufsAddress);

    JournalFactory journalFactory = new JournalFactory.ReadWrite(journalAddress);
    mBlockMaster = new BlockMaster(journalFactory);
    ExecutorService service =
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("FileSystemMasterTest-%d", true));
    mFileSystemMaster = new FileSystemMaster(mBlockMaster, journalFactory,
        ExecutorServiceFactories.constantExecutorServiceFactory(service));

    mBlockMaster.start(true);
    mFileSystemMaster.start(true);

    PowerMockito.mockStatic(UnderFileSystem.Factory.class);
    PowerMockito.when(UnderFileSystem.Factory.get(Mockito.anyString())).thenAnswer(
        new Answer<UnderFileSystem>() {
          @Override
          public UnderFileSystem answer(InvocationOnMock invocationOnMock) throws Throwable {
            AlluxioURI uri = new AlluxioURI((String) invocationOnMock.getArguments()[0]);
            return new SleepingLocalUnderFileSystem(uri, SLEEP_MS);
          }
        });
  }

  /**
   * Resets global state after each test run.
   */
  @After
  public void after() throws Exception {
    mFileSystemMaster.stop();
    mBlockMaster.stop();
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * A under storage implementation which sleeps before each rename call and no-ops directory
   * creation. The goal of this ufs is to inject sleeps into the rename code path without
   * modifying the underlying local file system.
   */
  public class SleepingLocalUnderFileSystem extends LocalUnderFileSystem {
    private final long mSleepMs;

    public SleepingLocalUnderFileSystem(AlluxioURI uri, long sleepMs) {
      super(uri);
      mSleepMs = sleepMs;
    }

    @Override
    public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
      return true;
    }

    @Override
    public boolean renameDirectory(String src, String dst) throws IOException {
      CommonUtils.sleepMs(mSleepMs);
      return true;
    }

    @Override
    public boolean renameFile(String src, String dst) throws IOException {
      CommonUtils.sleepMs(mSleepMs);
      return true;
    }
  }

  /**
   * Uses the integer suffix of a path to determine order. Paths without integer suffixes will be
   * ordered last.
   */
  private class IntegerSuffixedPathComparator implements Comparator<FileInfo> {
    @Override
    public int compare(FileInfo o1, FileInfo o2) {
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

  /**
   * Tests concurrent renames within the root do not block on each other.
   */
  @Test
  public void rootConcurrentRename() throws Exception {
    final int numThreads = 100;
    AlluxioURI[] srcs = new AlluxioURI[numThreads];
    AlluxioURI[] dsts = new AlluxioURI[numThreads];

    for (int i = 0; i < numThreads; i++) {
      srcs[i] = new AlluxioURI("/file" + i);
      mFileSystemMaster.createFile(srcs[i], sCreatePersistedFileOptions);
      dsts[i] = new AlluxioURI("/renamed" + i);
    }

    int errors = concurrentRename(srcs, dsts);

    Assert.assertEquals("More than 0 errors: " + errors, 0, errors);
    List<FileInfo> files =
        mFileSystemMaster.listStatus(new AlluxioURI("/"), ListStatusOptions.defaults());
    Collections.sort(files, new IntegerSuffixedPathComparator());
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
    final int numThreads = 100;
    AlluxioURI[] srcs = new AlluxioURI[numThreads];
    AlluxioURI[] dsts = new AlluxioURI[numThreads];

    AlluxioURI dir = new AlluxioURI("/dir");

    mFileSystemMaster.createDirectory(dir, CreateDirectoryOptions.defaults());

    for (int i = 0; i < numThreads; i++) {
      srcs[i] = dir.join("/file" + i);
      mFileSystemMaster.createFile(srcs[i], sCreatePersistedFileOptions);
      dsts[i] = dir.join("/renamed" + i);
    }
    int errors = concurrentRename(srcs, dsts);

    Assert.assertEquals("More than 0 errors: " + errors, 0, errors);
    List<FileInfo> files =
        mFileSystemMaster.listStatus(dir, ListStatusOptions.defaults());
    Collections.sort(files, new IntegerSuffixedPathComparator());
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
    int numThreads = 100;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    for (int i = 0; i < numThreads; i++) {
      srcs[i] = new AlluxioURI("/file");
      dsts[i] = new AlluxioURI("/renamed" + i);
    }

    // Create the one source file
    mFileSystemMaster.createFile(srcs[0], sCreatePersistedFileOptions);

    int errors = concurrentRename(srcs, dsts);

    // We should get an error for all but 1 rename
    Assert.assertEquals(numThreads - 1, errors);

    List<FileInfo> files =
        mFileSystemMaster.listStatus(new AlluxioURI("/"), ListStatusOptions.defaults());

    // Only one renamed file should exist
    Assert.assertEquals(1, files.size());
    Assert.assertTrue(files.get(0).getName().startsWith("renamed"));
  }

  /**
   * Tests that many threads concurrently renaming the same directory will only succeed once.
   */
  @Test
  public void sameDirConcurrentRename() throws Exception {
    int numThreads = 100;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    for (int i = 0; i < numThreads; i++) {
      srcs[i] = new AlluxioURI("/dir");
      dsts[i] = new AlluxioURI("/renamed" + i);
    }

    // Create the one source directory
    mFileSystemMaster.createDirectory(srcs[0], CreateDirectoryOptions.defaults());
    mFileSystemMaster.createFile(new AlluxioURI("/dir/file"), sCreatePersistedFileOptions);

    int errors = concurrentRename(srcs, dsts);

    // We should get an error for all but 1 rename
    Assert.assertEquals(numThreads - 1, errors);
    // Only one renamed dir should exist
    List<FileInfo> existingDirs =
        mFileSystemMaster.listStatus(new AlluxioURI("/"), ListStatusOptions.defaults());
    Assert.assertEquals(1, existingDirs.size());
    Assert.assertTrue(existingDirs.get(0).getName().startsWith("renamed"));
    // The directory should contain the file
    List<FileInfo> dirChildren =
        mFileSystemMaster.listStatus(new AlluxioURI(existingDirs.get(0).getPath()),
            ListStatusOptions.defaults());
    Assert.assertEquals(1, dirChildren.size());
  }

  /**
   * Tests renaming files concurrently to the same destination will only succeed once.
   */
  @Test
  public void sameDstConcurrentRename() throws Exception {
    int numThreads = 100;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    for (int i = 0; i < numThreads; i++) {
      srcs[i] = new AlluxioURI("/file" + i);
      mFileSystemMaster.createFile(srcs[i], sCreatePersistedFileOptions);
      dsts[i] = new AlluxioURI("/renamed");
    }

    int errors = concurrentRename(srcs, dsts);

    // We should get an error for all but 1 rename.
    Assert.assertEquals(numThreads - 1, errors);

    List<FileInfo> files =
        mFileSystemMaster.listStatus(new AlluxioURI("/"), ListStatusOptions.defaults());
    // Store file names in a set to ensure the names are all unique.
    Set<String> renamedFiles = new HashSet<>();
    Set<String> originalFiles = new HashSet<>();
    for (FileInfo file : files) {
      if (file.getName().startsWith("renamed")) {
        renamedFiles.add(file.getName());
      }
      if (file.getName().startsWith("file")) {
        originalFiles.add(file.getName());
      }
    }
    // One renamed file should exist, and 9 original source files
    Assert.assertEquals(numThreads, files.size());
    Assert.assertEquals(1, renamedFiles.size());
    Assert.assertEquals(numThreads - 1, originalFiles.size());
  }

  /**
   * Tests renaming files concurrently from one directory to another succeeds.
   */
  @Test
  public void twoDirConcurrentRename() throws Exception {
    int numThreads = 100;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    AlluxioURI dir1 = new AlluxioURI("/dir1");
    AlluxioURI dir2 = new AlluxioURI("/dir2");
    mFileSystemMaster.createDirectory(dir1, CreateDirectoryOptions.defaults());
    mFileSystemMaster.createDirectory(dir2, CreateDirectoryOptions.defaults());
    for (int i = 0; i < numThreads; i++) {
      srcs[i] = dir1.join("file" + i);
      mFileSystemMaster.createFile(srcs[i], sCreatePersistedFileOptions);
      dsts[i] = dir2.join("renamed" + i);
    }

    int errors = concurrentRename(srcs, dsts);

    // We should get no errors
    Assert.assertEquals(0, errors);

    List<FileInfo> dir1Files =
        mFileSystemMaster.listStatus(dir1, ListStatusOptions.defaults());
    List<FileInfo> dir2Files =
        mFileSystemMaster.listStatus(dir2, ListStatusOptions.defaults());

    Assert.assertEquals(0, dir1Files.size());
    Assert.assertEquals(numThreads, dir2Files.size());

    Collections.sort(dir2Files, new IntegerSuffixedPathComparator());
    for (int i = 0; i < numThreads; i++) {
      Assert.assertEquals(dsts[i].getName(), dir2Files.get(i).getName());
    }
  }

  /**
   * Tests renaming files concurrently from and to two directories succeeds.
   */
  @Test
  public void acrossDirConcurrentRename() throws Exception {
    int numThreads = 100;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    AlluxioURI dir1 = new AlluxioURI("/dir1");
    AlluxioURI dir2 = new AlluxioURI("/dir2");
    mFileSystemMaster.createDirectory(dir1, CreateDirectoryOptions.defaults());
    mFileSystemMaster.createDirectory(dir2, CreateDirectoryOptions.defaults());
    for (int i = 0; i < numThreads; i++) {
      // Dir1 has even files, dir2 has odd files.
      if (i % 2 == 0) {
        srcs[i] = dir1.join("file" + i);
        dsts[i] = dir2.join("renamed" + i);
      } else {
        srcs[i] = dir2.join("file" + i);
        dsts[i] = dir1.join("renamed" + i);
      }
      mFileSystemMaster.createFile(srcs[i], sCreatePersistedFileOptions);
    }

    int errors = concurrentRename(srcs, dsts);

    // We should get no errors.
    Assert.assertEquals(0, errors);

    List<FileInfo> dir1Files =
        mFileSystemMaster.listStatus(dir1, ListStatusOptions.defaults());
    List<FileInfo> dir2Files =
        mFileSystemMaster.listStatus(dir2, ListStatusOptions.defaults());

    Assert.assertEquals(numThreads / 2, dir1Files.size());
    Assert.assertEquals(numThreads / 2, dir2Files.size());

    Collections.sort(dir1Files, new IntegerSuffixedPathComparator());
    for (int i = 1; i < numThreads; i += 2) {
      Assert.assertEquals(dsts[i].getName(), dir1Files.get(i / 2).getName());
    }

    Collections.sort(dir2Files, new IntegerSuffixedPathComparator());
    for (int i = 0; i < numThreads; i += 2) {
      Assert.assertEquals(dsts[i].getName(), dir2Files.get(i / 2).getName());
    }
  }

  /**
   * Helper for renaming a list of paths concurrently. Assumes the srcs are already created and
   * dsts do not exist. Enforces that the run time of this method is not greater than twice the
   * sleep time (to infer concurrent operations).
   *
   * @param src list of source paths
   * @param dst list of destination paths
   * @return how many errors occurred
   */
  private int concurrentRename(final AlluxioURI[] src, final AlluxioURI[] dst)
      throws Exception {
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
            mFileSystemMaster.rename(src[iteration], dst[iteration]);
          } catch (Exception e) {
            Throwables.propagate(e);
          }
        }
      });
      t.setUncaughtExceptionHandler(exceptionHandler);
      threads.add(t);
    }
    Collections.shuffle(threads);
    long startNs = System.nanoTime();
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    long durationMs = (System.nanoTime() - startNs) / Constants.SECOND_NANO * Constants.SECOND_MS;
    Assert.assertTrue("Concurrent rename took " + durationMs, durationMs < 2 * SLEEP_MS);
    return errors.size();
  }

  /**
   * Tests that getFileInfo (read operation) either returns the correct file info or fails if it
   * has been renamed while the operation was waiting for the file lock.
   */
  @Test
  public void consistentGetFileInfo() throws Exception {
    final int iterations = 100;
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
      // Don't want sleeping ufs behavior, so do not set persisted flag
      mFileSystemMaster.createFile(file, CreateFileOptions.defaults());
      Thread renamer = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            AuthenticatedClientUser.set(TEST_USER);
            barrier.await();
            mFileSystemMaster.rename(file, dst);
            mFileSystemMaster.delete(dst, true);
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
            FileInfo info = mFileSystemMaster.getFileInfo(file);
            // If the file info is successfully obtained, then the path should match
            Assert.assertEquals(file.getName(), info.getName());
          } catch (InvalidPathException | FileDoesNotExistException e) {
            // InvalidPathException - if the file is renamed while the thread waits for the lock.
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
