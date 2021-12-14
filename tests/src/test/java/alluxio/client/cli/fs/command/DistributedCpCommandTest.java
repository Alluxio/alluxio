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

package alluxio.client.cli.fs.command;

import static org.junit.Assert.assertEquals;

import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.util.io.PathUtils;

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for cross-mount {@link alluxio.cli.fs.command.DistributedCpCommand}.
 */
public final class DistributedCpCommandTest extends AbstractFileSystemShellTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void crossMountCopy() throws Exception {
    File file = mFolder.newFile();
    Files.write("hello".getBytes(), file);
    run("mount", "/cross", mFolder.getRoot().getAbsolutePath());
    run("ls", "-f", "/cross");
    run("distributedCp", PathUtils.concatPath("/cross", file.getName()), "/copied");
    mOutput.reset();
    run("cat", "/copied");
    assertEquals("hello", mOutput.toString());
  }

  @Test
  public void crossMountCopyWithBatch() throws Exception {
    File file = mFolder.newFile();
    File file2 = mFolder.newFile();
    Files.write("hello".getBytes(), file);
    Files.write("world".getBytes(), file2);
    run("mount", "/cross", mFolder.getRoot().getAbsolutePath());
    run("ls", "-f", "/cross");
    run("distributedCp", "--batch-size", "2", "/cross", "/copied");
    mOutput.reset();
    run("cat", PathUtils.concatPath("/copied", file.getName()));
    assertEquals("hello", mOutput.toString());
    mOutput.reset();
    run("cat", PathUtils.concatPath("/copied", file2.getName()));
    assertEquals("world", mOutput.toString());
  }

  @Test
  public void crossMountCopyLotsFilesWithSmallBatchSize() throws Exception {
    int fileSize = 100;
    List<File> files = new ArrayList<>(fileSize);
    for (int i = 0; i < fileSize; i++) {
      File file = mFolder.newFile();
      String content = "hello" + i;
      Files.write(content.getBytes(), file);
      files.add(file);
    }
    run("mount", "/cross", mFolder.getRoot().getAbsolutePath());
    run("ls", "-f", "/cross");
    run("distributedCp", "--batch-size", "3", "/cross", "/copied");
    for (int i = 0; i < fileSize; i++) {
      mOutput.reset();
      run("cat", PathUtils.concatPath("/copied", files.get(i).getName()));
      assertEquals("hello" + i, mOutput.toString());
    }
  }

  @Test
  public void crossMountCopyNestedFilesWithBatch() throws Exception {
    int fileSize = 10;
    List<File> files = new ArrayList<>(fileSize);
    List<File> subFolderFiles = new ArrayList<>(fileSize);
    List<File> subSubFolderFiles = new ArrayList<>(fileSize);
    File subDir = mFolder.newFolder("subFolder");
    File subSubDir = mFolder.newFolder("subFolder", "subSubFolder");
    for (int i = 0; i < fileSize; i++) {
      File file = mFolder.newFile();
      String content = "hello" + i;
      Files.write(content.getBytes(), file);
      files.add(file);
      file = new File(subDir, "subFile" + i);
      content = "world" + i;
      Files.write(content.getBytes(), file);
      subFolderFiles.add(file);
      file = new File(subSubDir, "subSubFile" + i);
      content = "game" + i;
      Files.write(content.getBytes(), file);
      subSubFolderFiles.add(file);
    }
    run("mount", "/cross", mFolder.getRoot().getAbsolutePath());
    run("ls", "-f", "/cross");
    run("distributedCp", "--batch-size", "13", "/cross", "/copied");
    for (int i = 0; i < fileSize; i++) {
      mOutput.reset();
      run("cat", PathUtils.concatPath("/copied", files.get(i).getName()));
      assertEquals("hello" + i, mOutput.toString());
      mOutput.reset();
      run("cat",
          PathUtils.concatPath("/copied", subDir.getName(), subFolderFiles.get(i).getName()));
      assertEquals("world" + i, mOutput.toString());
      mOutput.reset();
      run("cat", PathUtils.concatPath("/copied", subDir.getName(), subSubDir.getName(),
          subSubFolderFiles.get(i).getName()));
      assertEquals("game" + i, mOutput.toString());
    }
  }

  private void run(String ...args) {
    if (sFsShell.run(args) != 0) {
      throw new RuntimeException(
          "Failed command <" + Joiner.on(" ").join(args) + "> " + mOutput.toString());
    }
  }
}
