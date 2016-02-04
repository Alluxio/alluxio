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

package tachyon.shell.command;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.exception.TachyonException;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.shell.TfsShellUtilsTest;

/**
 * Tests for rmr command.
 */
public class RmrCommandTest extends AbstractTfsShellTest {
  @Test
  public void rmrTest() throws IOException {
    StringBuilder toCompare = new StringBuilder();
    mFsShell.run("mkdir", "/testFolder1/testFolder2");
    toCompare.append(getCommandOutput(new String[] {"mkdir", "/testFolder1/testFolder2"}));
    mFsShell.run("touch", "/testFolder1/testFolder2/testFile2");
    toCompare
        .append(getCommandOutput(new String[] {"touch", "/testFolder1/testFolder2/testFile2"}));
    TachyonURI testFolder1 = new TachyonURI("/testFolder1");
    TachyonURI testFolder2 = new TachyonURI("/testFolder1/testFolder2");
    TachyonURI testFile2 = new TachyonURI("/testFolder1/testFolder2/testFile2");
    Assert.assertTrue(fileExist(testFolder1));
    Assert.assertTrue(fileExist(testFolder2));
    Assert.assertTrue(fileExist(testFile2));
    mFsShell.run("rmr", "/testFolder1/testFolder2/testFile2");
    toCompare.append("WARNING: rmr is deprecated. Please use rm -R instead.\n");
    toCompare.append(getCommandOutput(new String[] {"rm", "/testFolder1/testFolder2/testFile2"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertTrue(fileExist(testFolder1));
    Assert.assertTrue(fileExist(testFolder2));
    Assert.assertFalse(fileExist(testFile2));
    mFsShell.run("rmr", "/testFolder1");
    toCompare.append("WARNING: rmr is deprecated. Please use rm -R instead.\n");
    toCompare.append(getCommandOutput(new String[] {"rmr", "/testFolder1"}));
    Assert.assertEquals(toCompare.toString(), mOutput.toString());
    Assert.assertFalse(fileExist(testFolder1));
    Assert.assertFalse(fileExist(testFolder2));
    Assert.assertFalse(fileExist(testFile2));
  }

  @Test
  public void rmrWildCardTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mFileSystem);

    mFsShell.run("rmr", "/testWildCards/foo/foo*");
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foo/foobar1")));
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foo/foobar2")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/foo")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/bar/foobar3")));

    mFsShell.run("rmr", "/testWildCards/ba*");
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/bar")));
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/bar/foobar3")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/foobar4")));

    mFsShell.run("rmr", "/testWildCards/*");
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/bar")));
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foo")));
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foobar4")));
  }
}
