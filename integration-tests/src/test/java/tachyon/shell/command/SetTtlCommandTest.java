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
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.WriteType;
import tachyon.shell.AbstractTfsShellTest;

/**
 * Tests for setTtl command.
 */
public class SetTtlCommandTest extends AbstractTfsShellTest {
  @Test
  public void setTtlTest() throws Exception {
    String filePath = "/testFile";
    TachyonFSTestUtils.createByteFile(mTfs, filePath, WriteType.MUST_CACHE, 1);
    Assert.assertEquals(Constants.NO_TTL, mTfs.getStatus(new TachyonURI("/testFile")).getTtl());
    long[] ttls = new long[] { 0L, 1000L };
    for (long ttl : ttls) {
      Assert.assertEquals(0, mFsShell.run("setTtl", filePath, String.valueOf(ttl)));
      Assert.assertEquals(ttl, mTfs.getStatus(new TachyonURI("/testFile")).getTtl());
    }
  }

  @Test
  public void setTTLNegativeTest() throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, 1);
    mException.expect(IllegalArgumentException.class);
    mException.expectMessage("TTL value must be >= 0");
    mFsShell.run("setTtl", "/testFile", "-1");
  }
}
