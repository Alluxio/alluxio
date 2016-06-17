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

import alluxio.AlluxioURI;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for the {@link FileUtils} class.
 */
public class FileUtilsTest {

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
  public void changeLocalFilePermissionTest() throws IOException {
    File tempFile = mTestFolder.newFile("perm.txt");
    FileUtils.changeLocalFilePermission(tempFile.getAbsolutePath(), "---------");
    Assert.assertFalse(tempFile.canRead() || tempFile.canWrite() || tempFile.canExecute());
    FileUtils.changeLocalFilePermission(tempFile.getAbsolutePath(), "rwxrwxrwx");
    Assert.assertTrue(tempFile.canRead() && tempFile.canWrite() && tempFile.canExecute());
    // File deletion should fail, because we don't have write permissions
    FileUtils.changeLocalFilePermission(tempFile.getAbsolutePath(), "r--r--r--");
    Assert.assertTrue(tempFile.canRead());
    Assert.assertFalse(tempFile.canWrite());
    Assert.assertFalse(tempFile.canExecute());
    // expect a file permission error when we open it for writing
    mException.expect(IOException.class);
    @SuppressWarnings({"unused", "resource"})
    FileWriter fw = new FileWriter(tempFile);
    Assert.fail("opening a read-only file for writing should have failed");
  }

  /**
   * Tests the {@link FileUtils#changeLocalFilePermission(String, String)} method for a non-existent
   * file to thrown an exception.
   */
  @Test
  public void changeNonExistentFileTest() throws IOException {
    // ghostFile is never created, so changing permission should fail
    File ghostFile = new File(mTestFolder.getRoot(), "ghost.txt");
    mException.expect(IOException.class);
    FileUtils.changeLocalFilePermission(ghostFile.getAbsolutePath(), "rwxrwxrwx");
    Assert.fail("changing permissions of a non-existent file should have failed");
  }

  /**
   * Tests the {@link FileUtils#changeLocalFilePermission(String, String)} method for a directory.
   */
  @Test
  public void changeLocalDirPermissionTests() throws IOException {
    File tempFile = mTestFolder.newFile("perm.txt");
    // Change permission on directories
    FileUtils.changeLocalFilePermission(mTestFolder.getRoot().getAbsolutePath(), "r--r--r--");
    Assert.assertFalse(tempFile.delete());
    FileUtils.changeLocalFilePermission(mTestFolder.getRoot().getAbsolutePath(), "rwxr--r--");
    Assert.assertTrue(tempFile.delete());
  }

  /**
   * Tests the {@link FileUtils#move(String, String)} method.
   */
  @Test
  public void moveFileTest() throws IOException {
    File fromFile = mTestFolder.newFile("from.txt");
    File toFile = mTestFolder.newFile("to.txt");
    // Move a file and verify
    FileUtils.move(fromFile.getAbsolutePath(), toFile.getAbsolutePath());
    Assert.assertFalse(fromFile.exists());
    Assert.assertTrue(toFile.exists());
  }

  /**
   * Tests the {@link FileUtils#move(String, String)} method to thrown an exception when trying to
   * move a non-existent file.
   */
  @Test
  public void moveNonExistentFileTest() throws IOException {
    // ghostFile is never created, so deleting should fail
    File ghostFile = new File(mTestFolder.getRoot(), "ghost.txt");
    File toFile = mTestFolder.newFile("to.txt");
    mException.expect(IOException.class);
    FileUtils.move(ghostFile.getAbsolutePath(), toFile.getAbsolutePath());
    Assert.fail("moving a non-existent file should have failed");
  }

  /**
   * Tests the {@link FileUtils#delete(String)} method when trying to delete a file and a directory.
   */
  @Test
  public void deleteFileTest() throws IOException {
    File tempFile = mTestFolder.newFile("fileToDelete");
    File tempFolder = mTestFolder.newFolder("dirToDelete");
    // Delete a file and a directory
    FileUtils.delete(tempFile.getAbsolutePath());
    FileUtils.delete(tempFolder.getAbsolutePath());
    Assert.assertFalse(tempFile.exists());
    Assert.assertFalse(tempFolder.exists());
  }

  /**
   * Tests the {@link FileUtils#deletePathRecursively(String)} method when trying to delete
   * directories.
   */
  @Test
  public void deletePathRecursivelyTest() throws IOException {
    File tmpDir = mTestFolder.newFolder("dir");
    File tmpDir1 = mTestFolder.newFolder("dir", "dir1");
    File tmpDir2 = mTestFolder.newFolder("dir", "dir2");

    File tmpFile1 = mTestFolder.newFile("dir/dir1/file1");
    File tmpFile2 = mTestFolder.newFile("dir/dir1/file2");
    File tmpFile3 = mTestFolder.newFile("dir/file3");

    // Delete all of these.
    FileUtils.deletePathRecursively(tmpDir.getAbsolutePath());

    Assert.assertFalse(tmpDir.exists());
    Assert.assertFalse(tmpDir1.exists());
    Assert.assertFalse(tmpDir2.exists());
    Assert.assertFalse(tmpFile1.exists());
    Assert.assertFalse(tmpFile2.exists());
    Assert.assertFalse(tmpFile3.exists());
  }

  /**
   * Tests the {@link FileUtils#delete(String)} method to throw an exception when trying to delete a
   * non-existent file.
   */
  @Test
  public void deleteNonExistentFileTest() throws IOException {
    // ghostFile is never created, so deleting should fail
    File ghostFile = new File(mTestFolder.getRoot(), "ghost.txt");
    mException.expect(IOException.class);
    FileUtils.delete(ghostFile.getAbsolutePath());
    Assert.fail("deleting a non-existent file should have failed");
  }

  /**
   * Tests the {@link FileUtils#setLocalDirStickyBit(String)} method.
   */
  @Test
  public void setLocalDirStickyBitTest() throws IOException {
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
        Assert.assertTrue(line.matches("^d[rwx-]{8}t.*$"));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Tests the {@link FileUtils#createBlockPath(String)} method.
   */
  @Test
  public void createBlockPathTest() throws IOException {
    String absolutePath = PathUtils.concatPath(mTestFolder.getRoot(), "tmp", "bar");
    File tempFile = new File(absolutePath);
    FileUtils.createBlockPath(tempFile.getAbsolutePath());
    Assert.assertTrue(FileUtils.exists(tempFile.getParent()));
  }

  /**
   * Tests the {@link FileUtils#createFile(String)} method.
   */
  @Test
  public void createFileTest() throws IOException {
    File tempFile = new File(mTestFolder.getRoot(), "tmp");
    FileUtils.createFile(tempFile.getAbsolutePath());
    Assert.assertTrue(FileUtils.exists(tempFile.getAbsolutePath()));
    Assert.assertTrue(tempFile.delete());
  }

  /**
   * Tests the {@link FileUtils#createDir(String)} method.
   */
  @Test
  public void createDirTest() throws IOException {
    File tempDir = new File(mTestFolder.getRoot(), "tmp");
    FileUtils.createDir(tempDir.getAbsolutePath());
    Assert.assertTrue(FileUtils.exists(tempDir.getAbsolutePath()));
    Assert.assertTrue(tempDir.delete());
  }
}
