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

package alluxio.shell.command;

import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for fileInfo command.
 */
public final class FileInfoCommandIntegrationTest extends AbstractAlluxioShellTest {
  @Test
  public void fileInfoNotExist() throws IOException {
    mFsShell.run("fileInfo", "/NotExistFile");
    String res1 = mOutput.toString();
    Assert.assertTrue(res1.contains("The \"alluxio fs fileInfo <path>\" "
        + "command is deprecated since version 1.5."));
  }

  @Test
  public void fileInfoWildCard() throws IOException, AlluxioException {
    String testDir = AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);
    mFsShell.run("fileInfo", testDir + "/*");
    String res1 = mOutput.toString();
    Assert.assertTrue(res1.contains("The \"alluxio fs fileInfo <path>\" "
        + "command is deprecated since version 1.5."));
  }
}
