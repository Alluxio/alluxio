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
 * Test for rm with a wildcard argument
 */
public class RmWildCardTest extends AbstractTfsShellTest {
  @Test
  public void rmWildCardTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);

    mFsShell.run("rm", "/testWildCards/foo/foo*");
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foo/foobar1")));
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foo/foobar2")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/bar/foobar3")));

    mFsShell.run("rm", "/testWildCards/*");
    Assert.assertFalse(fileExist(new TachyonURI("/testWildCards/foobar4")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/foo")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/bar")));
    Assert.assertTrue(fileExist(new TachyonURI("/testWildCards/bar/foobar3")));
  }
}
