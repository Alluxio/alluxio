/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for unsetTtl command.
 */
public final class UnsetTtlTest extends AbstractAlluxioShellTest {
  @Test
  public void unsetTtl() throws Exception {
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
