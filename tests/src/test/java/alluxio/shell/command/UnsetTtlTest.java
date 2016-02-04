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

import org.junit.Assert;
import org.junit.Test;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.shell.AbstractAlluxioShellTest;

/**
 * Test for unsetTtl command.
 */
public class UnsetTtlTest extends AbstractAlluxioShellTest {
  @Test
  public void unsetTtlTest() throws Exception {
    String filePath = "/testFile";
    AlluxioURI uri = new AlluxioURI("/testFile");
    FileSystemTestUtils.createByteFile(mFileSystem, filePath, WriteType.MUST_CACHE, 1);
    Assert.assertEquals(Constants.NO_TTL, mFileSystem.getStatus(uri).getTtl());

    // unsetTTL on a file originally with no TTL will leave the TTL unchanged.
    Assert.assertEquals(0, mFsShell.run("unsetTtl", filePath));
    Assert.assertEquals(Constants.NO_TTL, mFileSystem.getStatus(uri).getTtl());

    long ttl = 1000L;
    Assert.assertEquals(0, mFsShell.run("setTtl", filePath, String.valueOf(ttl)));
    Assert.assertEquals(ttl, mFileSystem.getStatus(uri).getTtl());
    Assert.assertEquals(0, mFsShell.run("unsetTtl", filePath));
    Assert.assertEquals(Constants.NO_TTL, mFileSystem.getStatus(uri).getTtl());
  }
}
