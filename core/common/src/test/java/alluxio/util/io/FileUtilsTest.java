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

package alluxio.util.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

/**
 * Tests for the {@link FileUtils} class.
 */
public class FileUtilsTest {

  private String mWorkerDataFolderPerms = ConfigurationUtils.defaults()
      .get(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS);

  /**
   * The temporary folder.
   */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * The expected exception thrown during a test.
   */
  @Rule
  public final ExpectedException mException = ExpectedException.none();

  /**
   * Tests the {@link FileUtils#changeLocalFilePermission(String, String)} method.
   */
  @Test
  public void changeLocalFilePermission() throws IOException {
    // This test only works with normal users - superusers can operate on files whether or not they
    // have the proper permission bits set.
    assumeFalse(System.getProperty("user.name").equals("root"));
    File tempFile = mTestFolder.newFile("perm.txt");
    FileUtils.changeLocalFilePermission(tempFile.getAbsolutePath(), "---------");
    assertFalse(tempFile.canRead() || tempFile.canWrite() || tempFile.canExecute());
    FileUtils.changeLocalFilePermission(tempFile.getAbsolutePath(), "rwxrwxrwx");
    assertTrue(tempFile.canRead() && tempFile.canWrite() && tempFile.canExecute());
    // File deletion should fail, because we don't have write permissions
    FileUtils.changeLocalFilePermission(tempFile.getAbsolutePath(), "r--r--r--");
    assertTrue(tempFile.canRead());
    assertFalse(tempFile.canWrite());
    assertFalse(tempFile.canExecute());
    // expect a file permission error when we open it for writing
    mException.expect(IOException.class);
    @SuppressWarnings({"unused", "resource"})
    FileWriter fw = new FileWriter(tempFile);
    fail("opening a read-only file for writing should have failed");
  }

  /**
   * Tests the {@link FileUtils#changeLocalFilePermission(String, String)} method for a non-existent
   * file to thrown an exception.
   */
  @Test
  public void changeNonExistentFile() throws IOException {
    // ghostFile is never created, so changing permission should fail
    File ghostFile = new File(mTestFolder.getRoot(), "ghost.txt");
    mException.expect(IOException.class);
    FileUtils.changeLocalFilePermission(ghostFile.getAbsolutePath(), "rwxrwxrwx");
    fail("changing permissions of a non-existent file should have failed");
  }

  /**
   * Tests the {@link FileUtils#changeLocalFilePermission(String, String)} method for a directory.
   */
  @Test
  public void changeLocalDirPermissionTests() throws IOException {
    // This test only works with normal users - superusers can operate on files whether or not they
    // have the proper permission bits set.
    assumeFalse(System.getProperty("user.name").equals("root"));
    File tempFile = mTestFolder.newFile("perm.txt");
    // Change permission on directories
    FileUtils.changeLocalFilePermission(mTestFolder.getRoot().getAbsolutePath(), "r--r--r--");
    assertFalse(tempFile.delete());
    FileUtils.changeLocalFilePermission(mTestFolder.getRoot().getAbsolutePath(), "rwxr--r--");
    assertTrue(tempFile.delete());
  }

  /**
   * Tests the {@link FileUtils#move(String, String)} method.
   */
  @Test
  public void moveFile() throws IOException {
    File fromFile = mTestFolder.newFile("from.txt");
    File toFile = mTestFolder.newFile("to.txt");
    // Move a file and verify
    FileUtils.move(fromFile.getAbsolutePath(), toFile.getAbsolutePath());
    assertFalse(fromFile.exists());
    assertTrue(toFile.exists());
  }

  /**
   * Tests the {@link FileUtils#move(String, String)} method to thrown an exception when trying to
   * move a non-existent file.
   */
  @Test
  public void moveNonExistentFile() throws IOException {
    // ghostFile is never created, so deleting should fail
    File ghostFile = new File(mTestFolder.getRoot(), "ghost.txt");
    File toFile = mTestFolder.newFile("to.txt");
    mException.expect(IOException.class);
    FileUtils.move(ghostFile.getAbsolutePath(), toFile.getAbsolutePath());
    fail("moving a non-existent file should have failed");
  }

  /**
   * Tests the {@link FileUtils#delete(String)} method when trying to delete a file and a directory.
   */
  @Test
  public void deleteFile() throws IOException {
    File tempFile = mTestFolder.newFile("fileToDelete");
    File tempFolder = mTestFolder.newFolder("dirToDelete");
    // Delete a file and a directory
    FileUtils.delete(tempFile.getAbsolutePath());
    FileUtils.delete(tempFolder.getAbsolutePath());
    assertFalse(tempFile.exists());
    assertFalse(tempFolder.exists());
  }

  /**
   * Tests the {@link FileUtils#deletePathRecursively(String)} method when trying to delete
   * directories.
   */
  @Test
  public void deletePathRecursively() throws IOException {
    File tmpDir = mTestFolder.newFolder("dir");
    File tmpDir1 = mTestFolder.newFolder("dir", "dir1");
    File tmpDir2 = mTestFolder.newFolder("dir", "dir2");

    File tmpFile1 = mTestFolder.newFile("dir/dir1/file1");
    File tmpFile2 = mTestFolder.newFile("dir/dir1/file2");
    File tmpFile3 = mTestFolder.newFile("dir/file3");

    // Delete all of these.
    FileUtils.deletePathRecursively(tmpDir.getAbsolutePath());

    assertFalse(tmpDir.exists());
    assertFalse(tmpDir1.exists());
    assertFalse(tmpDir2.exists());
    assertFalse(tmpFile1.exists());
    assertFalse(tmpFile2.exists());
    assertFalse(tmpFile3.exists());
  }

  /**
   * Tests the {@link FileUtils#delete(String)} method to throw an exception when trying to delete a
   * non-existent file.
   */
  @Test
  public void deleteNonExistentFile() throws IOException {
    // ghostFile is never created, so deleting should fail
    File ghostFile = new File(mTestFolder.getRoot(), "ghost.txt");
    mException.expect(IOException.class);
    FileUtils.delete(ghostFile.getAbsolutePath());
    fail("deleting a non-existent file should have failed");
  }

  /**
   * Tests the {@link FileUtils#setLocalDirStickyBit(String)} method.
   */
  @Test
  public void setLocalDirStickyBit() throws IOException {
    File tempFolder = mTestFolder.newFolder("dirToModify");
    // Only test this functionality of the absolute path of the temporary directory starts with "/",
    // which implies the host should support "chmod".
    if (tempFolder.getAbsolutePath().startsWith(AlluxioURI.SEPARATOR)) {
      FileUtils.setLocalDirStickyBit(tempFolder.getAbsolutePath());
      List<String> commands = new ArrayList<>();
      commands.add("/bin/ls");
      commands.add("-ld");
      commands.add(tempFolder.getAbsolutePath());
      try {
        ProcessBuilder builder = new ProcessBuilder(commands);
        Process process = builder.start();
        process.waitFor();
        BufferedReader stdInput = new BufferedReader(new
            InputStreamReader(process.getInputStream()));
        String line = stdInput.readLine();
        // we are just concerned about the first and the last permission bits
        assertTrue(line.matches("^d[rwx-]{8}t.*$"));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Tests the {@link FileUtils#createBlockPath(String)} method.
   */
  @Test
  public void createBlockPath() throws IOException {
    String absolutePath = PathUtils.concatPath(mTestFolder.getRoot(), "tmp", "bar");
    File tempFile = new File(absolutePath);
    FileUtils.createBlockPath(tempFile.getAbsolutePath(), mWorkerDataFolderPerms);
    assertTrue(FileUtils.exists(tempFile.getParent()));
  }

  /**
   * Tests the {@link FileUtils#createFile(String)} method.
   */
  @Test
  public void createFile() throws IOException {
    File tempFile = new File(mTestFolder.getRoot(), "tmp");
    FileUtils.createFile(tempFile.getAbsolutePath());
    assertTrue(FileUtils.exists(tempFile.getAbsolutePath()));
    assertTrue(tempFile.delete());
  }

  /**
   * Tests the {@link FileUtils#createDir(String)} method.
   */
  @Test
  public void createDir() throws IOException {
    File tempDir = new File(mTestFolder.getRoot(), "tmp");
    FileUtils.createDir(tempDir.getAbsolutePath());
    assertTrue(FileUtils.exists(tempDir.getAbsolutePath()));
    assertTrue(tempDir.delete());
  }

  /**
   * Tests the {@link FileUtils#getLocalFileMode(String)}} method.
   */
  @Test
  public void getLocalFileMode() throws IOException {
    File tmpDir = mTestFolder.newFolder("dir");
    File tmpFile777 = mTestFolder.newFile("dir/0777");
    tmpFile777.setReadable(true, false /* owner only */);
    tmpFile777.setWritable(true, false /* owner only */);
    tmpFile777.setExecutable(true, false /* owner only */);

    File tmpFile755 = mTestFolder.newFile("dir/0755");
    tmpFile755.setReadable(true, false /* owner only */);
    tmpFile755.setWritable(false, false /* owner only */);
    tmpFile755.setExecutable(true, false /* owner only */);
    tmpFile755.setWritable(true, true /* owner only */);

    File tmpFile444 = mTestFolder.newFile("dir/0444");
    tmpFile444.setReadOnly();

    assertEquals((short) 0777, FileUtils.getLocalFileMode(tmpFile777.getPath()));
    assertEquals((short) 0755, FileUtils.getLocalFileMode(tmpFile755.getPath()));
    assertEquals((short) 0444, FileUtils.getLocalFileMode(tmpFile444.getPath()));

    // Delete all of these.
    FileUtils.deletePathRecursively(tmpDir.getAbsolutePath());
  }

  /**
   * Tests {@link FileUtils#createBlockPath} method when storage dir exists or doesn't exist.
   */
  @Test
  public void createStorageDirPath() throws IOException {
    File storageDir = new File(mTestFolder.getRoot(), "storageDir");
    File blockFile = new File(storageDir, "200");

    // When storage dir doesn't exist
    FileUtils.createBlockPath(blockFile.getAbsolutePath(), mWorkerDataFolderPerms);
    assertTrue(FileUtils.exists(storageDir.getAbsolutePath()));
    assertEquals(
        PosixFilePermissions.fromString("rwxrwxrwx"),
        Files.getPosixFilePermissions(Paths.get(storageDir.getAbsolutePath())));

    // When storage dir exists
    FileUtils.createBlockPath(blockFile.getAbsolutePath(), mWorkerDataFolderPerms);
    assertTrue(FileUtils.exists(storageDir.getAbsolutePath()));
  }

  /**
   * Tests invoking {@link FileUtils#createBlockPath} method concurrently. This simulates the case
   * when multiple blocks belonging to the same storage dir get created concurrently.
   */
  @Test
  public void concurrentCreateStorageDirPath() throws Exception {
    /**
     * A class provides multiple concurrent threads to invoke {@link FileUtils#createBlockPath}.
     */
    class ConcurrentCreator implements Callable<Void> {
      private final String mPath;
      private final CyclicBarrier mBarrier;

      ConcurrentCreator(String path, CyclicBarrier barrier) {
        mPath = path;
        mBarrier = barrier;
      }

      @Override
      @Nullable
      public Void call() throws Exception {
        mBarrier.await(); // Await until all threads submitted
        FileUtils.createBlockPath(mPath, mWorkerDataFolderPerms);
        return null;
      }
    }

    final int numCreators = 5;
    List<Future<Void>> futures = new ArrayList<>(numCreators);
    for (int iteration = 0; iteration < 5; iteration++) {
      final ExecutorService executor = Executors.newFixedThreadPool(numCreators);
      final CyclicBarrier barrier = new CyclicBarrier(numCreators);
      try {
        File storageDir = new File(mTestFolder.getRoot(), "tmp" + iteration);
        for (int i = 0; i < numCreators; i++) {
          File blockFile = new File(storageDir, String.valueOf(i));
          futures.add(executor.submit(new ConcurrentCreator(blockFile.getAbsolutePath(), barrier)));
        }
        for (Future<Void> f : futures) {
          f.get();
        }
        assertTrue(FileUtils.exists(storageDir.getAbsolutePath()));
      } finally {
        executor.shutdown();
      }
    }
  }
}
