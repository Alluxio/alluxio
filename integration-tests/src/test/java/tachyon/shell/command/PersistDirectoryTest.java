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
import tachyon.exception.TachyonException;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.shell.TfsShellUtilsTest;

/**
 * Test used to persist a directory
 */
public class PersistDirectoryTest extends AbstractTfsShellTest {
  @Test
  public void persistDirectoryTest() throws IOException, TachyonException {
    // Set the default write type to MUST_CACHE, so that directories are not persisted by default
    ClientContext.getConf().set(Constants.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE");
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);
    Assert.assertFalse(mTfs.getInfo(mTfs.open(new TachyonURI("/testWildCards"))).isIsPersisted());
    Assert.assertFalse(
        mTfs.getInfo(mTfs.open(new TachyonURI("/testWildCards/foo"))).isIsPersisted());
    Assert.assertFalse(
        mTfs.getInfo(mTfs.open(new TachyonURI("/testWildCards/bar"))).isIsPersisted());
    int ret = mFsShell.run("persist", "/testWildCards");
    Assert.assertEquals(0, ret);
    Assert.assertTrue(mTfs.getInfo(mTfs.open(new TachyonURI("/testWildCards"))).isIsPersisted());
    Assert
        .assertTrue(mTfs.getInfo(mTfs.open(new TachyonURI("/testWildCards/foo"))).isIsPersisted());
    Assert
        .assertTrue(mTfs.getInfo(mTfs.open(new TachyonURI("/testWildCards/bar"))).isIsPersisted());
    checkFilePersisted(mTfs.open(new TachyonURI("/testWildCards/foo/foobar1")), 10);
    checkFilePersisted(mTfs.open(new TachyonURI("/testWildCards/foo/foobar2")), 20);
    checkFilePersisted(mTfs.open(new TachyonURI("/testWildCards/bar/foobar3")), 30);
    checkFilePersisted(mTfs.open(new TachyonURI("/testWildCards/foobar4")), 40);
    ClientContext.reset();
  }
}
