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

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.FileSystemTestUtils;
import tachyon.client.WriteType;
import tachyon.shell.AbstractTfsShellTest;

/**
 * Test for unsetTtl command.
 */
public class UnsetTtlTest extends AbstractTfsShellTest {
  @Test
  public void unsetTtlTest() throws Exception {
    String filePath = "/testFile";
    TachyonURI uri = new TachyonURI("/testFile");
    FileSystemTestUtils.createByteFile(mTfs, filePath, WriteType.MUST_CACHE, 1);
    Assert.assertEquals(Constants.NO_TTL, mTfs.getStatus(uri).getTtl());

    // unsetTTL on a file originally with no TTL will leave the TTL unchanged.
    Assert.assertEquals(0, mFsShell.run("unsetTtl", filePath));
    Assert.assertEquals(Constants.NO_TTL, mTfs.getStatus(uri).getTtl());

    long ttl = 1000L;
    Assert.assertEquals(0, mFsShell.run("setTtl", filePath, String.valueOf(ttl)));
    Assert.assertEquals(ttl, mTfs.getStatus(uri).getTtl());
    Assert.assertEquals(0, mFsShell.run("unsetTtl", filePath));
    Assert.assertEquals(Constants.NO_TTL, mTfs.getStatus(uri).getTtl());
  }
}
