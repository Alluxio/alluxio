/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.shell.command;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.exception.TachyonException;
import alluxio.shell.AbstractTfsShellTest;
import alluxio.shell.TfsShellUtilsTest;
import alluxio.util.io.BufferUtils;

/**
 * Tests for cat command.
 */
public class CatCommandTest extends AbstractTfsShellTest {
  @Test
  public void catDirectoryTest() throws IOException {
    String[] command = new String[] {"mkdir", "/testDir"};
    mFsShell.run(command);
    int ret = mFsShell.run("cat", "/testDir");
    Assert.assertEquals(-1, ret);
    String expected = getCommandOutput(command);
    expected += "Path /testDir must be a file\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void catNotExistTest() throws IOException {
    int ret = mFsShell.run("cat", "/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void catTest() throws IOException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 10);
    mFsShell.run("cat", "/testFile");
    byte[] expect = BufferUtils.getIncreasingByteArray(10);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void catWildcardTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mFileSystem);
    // the expect contents (remember that the order is based on path)
    byte[] exp1 = BufferUtils.getIncreasingByteArray(30); // testWildCards/bar/foobar3
    byte[] exp2 = BufferUtils.getIncreasingByteArray(10); // testWildCards/foo/foobar1
    byte[] exp3 = BufferUtils.getIncreasingByteArray(20); // testWildCards/foo/foobar2
    byte[] expect = new byte[exp1.length + exp2.length + exp3.length];
    System.arraycopy(exp1, 0, expect, 0, exp1.length);
    System.arraycopy(exp2, 0, expect, exp1.length, exp2.length);
    System.arraycopy(exp3, 0, expect, exp1.length + exp2.length, exp3.length);

    int ret = mFsShell.run("cat", "/testWildCards/*/foo*");
    Assert.assertEquals(0, ret);
    Assert.assertArrayEquals(mOutput.toByteArray(), expect);
  }
}
