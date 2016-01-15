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
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.shell.AbstractTfsShellTest;
import tachyon.shell.TfsShellUtilsTest;
import tachyon.util.CommonUtils;

/**
 * Test used to free folders with wildcard argument
 */
public class FreeWildCardTest extends AbstractTfsShellTest {
  @Test
  public void freeWildCardTest() throws IOException, TachyonException {
    TfsShellUtilsTest.resetTachyonFileHierarchy(mTfs);

    TachyonConf tachyonConf = mLocalTachyonCluster.getMasterTachyonConf();

    int ret = mFsShell.run("free", "/testWild*/foo/*");
    CommonUtils.sleepMs(null,
        tachyonConf.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS) * 2 + 10);
    Assert.assertEquals(0, ret);
    Assert.assertFalse(isInMemoryTest("/testWildCards/foo/foobar1"));
    Assert.assertFalse(isInMemoryTest("/testWildCards/foo/foobar2"));
    Assert.assertTrue(isInMemoryTest("/testWildCards/bar/foobar3"));
    Assert.assertTrue(isInMemoryTest("/testWildCards/foobar4"));

    ret = mFsShell.run("free", "/testWild*/*/");
    CommonUtils.sleepMs(null,
        tachyonConf.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS) * 2 + 10);
    Assert.assertEquals(0, ret);
    Assert.assertFalse(isInMemoryTest("/testWildCards/bar/foobar3"));
    Assert.assertFalse(isInMemoryTest("/testWildCards/foobar4"));
  }
}
