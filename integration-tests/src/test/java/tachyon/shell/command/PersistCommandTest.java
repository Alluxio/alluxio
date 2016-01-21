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

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.WriteType;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.TachyonException;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.shell.TfsShellUtilsTest;

/**
 * Tests for persist command.
 */
public class PersistCommandTest extends AbstractTfsShellTest {
  @Test
  public void persistTest() throws IOException, TachyonException {
    String testFilePath = "/testPersist/testFile";
    TachyonFSTestUtils.createByteFile(mTfs, testFilePath, WriteType.MUST_CACHE, 10);
    Assert.assertFalse(mTfs.getStatus(new TachyonURI("/testPersist/testFile")).isPersisted());

    int ret = mFsShell.run("persist", testFilePath);
    Assert.assertEquals(0, ret);
    Assert.assertEquals("persisted file " + testFilePath + " with size 10\n", mOutput.toString());
    checkFilePersisted(new TachyonURI("/testPersist/testFile"), 10);
  }

  @Test
  public void persistDirectoryTest() throws IOException, TachyonException {
    // Set the default write type to MUST_CACHE, so that directories are not persisted by default
    ClientContext.getConf().set(Constants.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);
    Assert.assertFalse(mTfs.getStatus(new TachyonURI("/testWildCards")).isPersisted());
    Assert
        .assertFalse(mTfs.getStatus(new TachyonURI("/testWildCards/foo")).isPersisted());
    Assert
        .assertFalse(mTfs.getStatus(new TachyonURI("/testWildCards/bar")).isPersisted());
    int ret = mFsShell.run("persist", "/testWildCards");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mTfs.getStatus(new TachyonURI("/testWildCards")).isPersisted());
    Assert
        .assertTrue(mTfs.getStatus(new TachyonURI("/testWildCards/foo")).isPersisted());
    Assert
        .assertTrue(mTfs.getStatus(new TachyonURI("/testWildCards/bar")).isPersisted());
    checkFilePersisted(new TachyonURI("/testWildCards/foo/foobar1"), 10);
    checkFilePersisted(new TachyonURI("/testWildCards/foo/foobar2"), 20);
    checkFilePersisted(new TachyonURI("/testWildCards/bar/foobar3"), 30);
    checkFilePersisted(new TachyonURI("/testWildCards/foobar4"), 40);
  }

  @Test
  public void persistNonexistentFileTest() throws IOException, TachyonException {
    // Cannot persist a nonexistent file
    String path = "/testPersistNonexistent";
    int ret = mFsShell.run("persist", path);
    Assert.assertEquals(-1, ret);
    Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path) + "\n",
        mOutput.toString());
  }

  @Test
  public void persistTwiceTest() throws IOException, TachyonException {
    // Persisting an already-persisted file is okay
    String testFilePath = "/testPersist/testFile";
    TachyonFSTestUtils.createByteFile(mTfs, testFilePath, WriteType.MUST_CACHE, 10);
    Assert.assertFalse(mTfs.getStatus(new TachyonURI("/testPersist/testFile")).isPersisted());
    int ret = mFsShell.run("persist", testFilePath);
    Assert.assertEquals(0, ret);
    ret = mFsShell.run("persist", testFilePath);
    Assert.assertEquals(0, ret);
    Assert.assertEquals("persisted file " + testFilePath + " with size 10\n" + testFilePath
        + " is already persisted\n", mOutput.toString());
    checkFilePersisted(new TachyonURI("/testPersist/testFile"), 10);
  }
}
