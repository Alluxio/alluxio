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

  private void run(String ...args) {
    if (sFsShell.run(args) != 0) {
      throw new RuntimeException(
          "Failed command <" + Joiner.on(" ").join(args) + "> " + mOutput.toString());
    }
  }
}
