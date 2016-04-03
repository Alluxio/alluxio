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

import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for fileInfo command.
 */
public class FileInfoCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void fileinfoNotExistTest() throws IOException {
    int ret = mFsShell.run("fileInfo", "/NotExistFile");
    Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/NotExistFile") + "\n",
        mOutput.toString());
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void fileinfoWildCardTest() throws IOException, AlluxioException {
    AlluxioShellUtilsTest.resetFileHierarchy(mFileSystem);

    mFsShell.run("fileInfo", "/testWildCards/*");
    String res1 = mOutput.toString();
    Assert.assertTrue(res1.contains("/testWildCards/foo"));
    Assert.assertTrue(res1.contains("/testWildCards/bar"));
    Assert.assertTrue(res1.contains("/testWildCards/foobar4"));
    Assert.assertFalse(res1.contains("/testWildCards/foo/foobar1"));
    Assert.assertFalse(res1.contains("/testWildCards/bar/foobar3"));

    mFsShell.run("fileInfo", "/testWildCards/*/foo*");
    String res2 = mOutput.toString();
    res2 = res2.replace(res1, "");
    Assert.assertTrue(res2.contains("/testWildCards/foo/foobar1"));
    Assert.assertTrue(res2.contains("/testWildCards/foo/foobar2"));
    Assert.assertTrue(res2.contains("/testWildCards/bar/foobar3"));
    Assert.assertFalse(res2.contains("/testWildCards/foobar4"));
  }
}
