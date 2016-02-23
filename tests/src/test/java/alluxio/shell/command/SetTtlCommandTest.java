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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.shell.AbstractAlluxioShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for setTtl command.
 */
public class SetTtlCommandTest extends AbstractAlluxioShellTest {
  @Test
  public void setTtlTest() throws Exception {
    String filePath = "/testFile";
    FileSystemTestUtils.createByteFile(mFileSystem, filePath, WriteType.MUST_CACHE, 1);
    Assert.assertEquals(Constants.NO_TTL, mFileSystem.getStatus(new AlluxioURI("/testFile"))
        .getTtl());
    long[] ttls = new long[] {0L, 1000L};
    for (long ttl : ttls) {
      Assert.assertEquals(0, mFsShell.run("setTtl", filePath, String.valueOf(ttl)));
      Assert.assertEquals(ttl, mFileSystem.getStatus(new AlluxioURI("/testFile")).getTtl());
    }
  }

  @Test
  public void setTtlNegativeTest() throws IOException {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, 1);
    mException.expect(IllegalArgumentException.class);
    mException.expectMessage("TTL value must be >= 0");
    mFsShell.run("setTtl", "/testFile", "-1");
  }
}
