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

import alluxio.exception.ExceptionMessage;
import alluxio.exception.AlluxioException;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtilsTest;

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
