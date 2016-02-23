/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for chgrpr command.
 */
public class ChgrprCommandTest extends AbstractAlluxioShellTest {

  @Test
  public void chgrprTest() throws IOException, AlluxioException {
    clearLoginUser();
    mFsShell.run("mkdir", "/testFolder1");
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFolder1/testFile", WriteType.MUST_CACHE,
        10);
    mFsShell.run("chgrpr", "group1", "/testFolder1");
    String group = mFileSystem.getStatus(new AlluxioURI("/testFolder1/testFile")).getGroupName();
    Assert.assertEquals("group1", group);
    mFsShell.run("chgrpr", "group2", "/testFolder1");
    group = mFileSystem.getStatus(new AlluxioURI("/testFolder1/testFile")).getGroupName();
    Assert.assertEquals("group2", group);
  }
}
