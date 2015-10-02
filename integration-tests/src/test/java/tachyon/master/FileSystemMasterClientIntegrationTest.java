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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.client.FileSystemMasterClient;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;

/**
 * Test the internal implementation of tachyon Master via a
 * {@link tachyon.client.FileSystemMasterClient}.
 *
 * <p>
 */
public class FileSystemMasterClientIntegrationTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private final ExecutorService mExecutorService = Executors.newFixedThreadPool(2);
  private TachyonConf mMasterTachyonConf = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    mExecutorService.shutdown();
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(1000, 1000, Constants.GB);
    mLocalTachyonCluster.start();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
  }

  @Test
  public void openCloseTest() throws TachyonException, IOException {
    FileSystemMasterClient fsMasterClient =
        new FileSystemMasterClient(mLocalTachyonCluster.getMaster().getAddress(), mExecutorService,
            mMasterTachyonConf);
    Assert.assertFalse(fsMasterClient.isConnected());
    fsMasterClient.connect();
    Assert.assertTrue(fsMasterClient.isConnected());
    fsMasterClient.createFile("/file", Constants.DEFAULT_BLOCK_SIZE_BYTE, true, Constants.NO_TTL);
    Assert.assertTrue(fsMasterClient.getFileInfo(fsMasterClient.getFileId("/file")) != null);
    fsMasterClient.disconnect();
    Assert.assertFalse(fsMasterClient.isConnected());
    fsMasterClient.connect();
    Assert.assertTrue(fsMasterClient.isConnected());
    Assert.assertTrue(fsMasterClient.getFileInfo(fsMasterClient.getFileId("/file")) != null);
    fsMasterClient.close();
  }

  @Test(timeout = 3000, expected = TachyonException.class)
  public void user_getClientBlockInfoReturnsOnError() throws IOException, TachyonException {
    // This test was created to show that an infinite loop occurs.
    // The timeout will protect against this, and the change was to throw a IOException
    // in the cases we don't want to disconnect from master
    FileSystemMasterClient fsMasterClient =
        new FileSystemMasterClient(mLocalTachyonCluster.getMaster().getAddress(), mExecutorService,
            mMasterTachyonConf);
    fsMasterClient.getFileInfo(Long.MAX_VALUE);
    fsMasterClient.close();
  }

  // TODO(gene): Cannot find counterpart for {@link MasterClientBase#user_getWorker} in new master
  // clients.
  // @Test(timeout = 3000, expected = NoWorkerException.class)
  // public void user_getWorkerReturnsWhenNotLocal() throws Exception {
  // // This test was created to show that an infinite loop occurs.
  // // The timeout will protect against this, and the change was to throw a IOException
  // // in the cases we don't want to disconnect from master
  // MasterClientBase masterClient =
  // new MasterClientBase(mMasterInfo.getMasterAddress(), mExecutorService, mMasterTachyonConf);
  // masterClient.user_getWorker(false, "host.doesnotexist.fail");
  // masterClient.close();
  // }
}
