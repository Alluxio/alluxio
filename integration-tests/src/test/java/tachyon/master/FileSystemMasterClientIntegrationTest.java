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

package tachyon.master;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.client.file.FileSystemMasterClient;
import tachyon.client.file.options.CreateOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;

/**
 * Test the internal implementation of tachyon Master via a
 * {@link FileSystemMasterClient}.
 *
 * <p>
 */
public class FileSystemMasterClientIntegrationTest {
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(1000, 1000, Constants.GB);
  private TachyonConf mMasterTachyonConf = null;

  @Before
  public final void before() throws Exception {
    mMasterTachyonConf = mLocalTachyonClusterResource.get().getMasterTachyonConf();
  }

  @Test
  public void openCloseTest() throws TachyonException, IOException {
    FileSystemMasterClient fsMasterClient = new FileSystemMasterClient(
        mLocalTachyonClusterResource.get().getMaster().getAddress(), mMasterTachyonConf);
    Assert.assertFalse(fsMasterClient.isConnected());
    fsMasterClient.connect();
    Assert.assertTrue(fsMasterClient.isConnected());
    fsMasterClient.create("/file", CreateOptions.defaults());
    Assert.assertTrue(fsMasterClient.getStatus(fsMasterClient.getFileId("/file")) != null);
    fsMasterClient.disconnect();
    Assert.assertFalse(fsMasterClient.isConnected());
    fsMasterClient.connect();
    Assert.assertTrue(fsMasterClient.isConnected());
    Assert.assertTrue(fsMasterClient.getStatus(fsMasterClient.getFileId("/file")) != null);
    fsMasterClient.close();
  }

  @Test(timeout = 3000, expected = TachyonException.class)
  public void getFileInfoReturnsOnErrorTest() throws IOException, TachyonException {
    // This test was created to show that an infinite loop occurs.
    // The timeout will protect against this, and the change was to throw a IOException
    // in the cases we don't want to disconnect from master
    FileSystemMasterClient fsMasterClient = new FileSystemMasterClient(
        mLocalTachyonClusterResource.get().getMaster().getAddress(), mMasterTachyonConf);
    fsMasterClient.getStatus(Long.MAX_VALUE);
    fsMasterClient.close();
  }
}
