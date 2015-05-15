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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NoWorkerException;

/**
 * Though its name indicates that it provide the unit tests for {@link tachyon.master.MasterClient},
 * this class is more like unit-testing the internal implementation of tachyon Master via a
 * {@link MasterClient}, and thus it depends on many components in tachyon.master. As a result, we
 * have MasterClient in tachyon-common and leave this class in tachyon-core.
 *
 * <p>
 * TODO: Rename this class.
 *
 */
public class MasterClientTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private MasterInfo mMasterInfo = null;
  private final ExecutorService mExecutorService = Executors.newFixedThreadPool(2);
  private TachyonConf mMasterTachyonConf = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    mExecutorService.shutdown();
  }

  @Before
  public final void before() throws IOException {
    mLocalTachyonCluster = new LocalTachyonCluster(1000, 1000, Constants.GB);
    mLocalTachyonCluster.start();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
  }

  @Test
  public void openCloseTest() throws FileAlreadyExistException, InvalidPathException, TException,
      IOException {
    MasterClient masterClient =
        new MasterClient(mMasterInfo.getMasterAddress(), mExecutorService, mMasterTachyonConf);
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.user_createFile("/file", "", Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertTrue(masterClient.getFileStatus(-1, "/file") != null);
    masterClient.disconnect();
    Assert.assertFalse(masterClient.isConnected());
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    Assert.assertTrue(masterClient.getFileStatus(-1, "/file") != null);
    masterClient.close();
  }

  @Test(timeout = 3000, expected = FileNotFoundException.class)
  public void user_getClientBlockInfoReturnsOnError() throws TException, IOException {
    // this test was created to show that a infi loop happens
    // the timeout will protect against this, and the change was to throw a IOException
    // in the cases we don't want to disconnect from master
    MasterClient masterClient =
        new MasterClient(mMasterInfo.getMasterAddress(), mExecutorService, mMasterTachyonConf);
    masterClient.user_getClientBlockInfo(Long.MAX_VALUE);
    masterClient.close();
  }

  @Test(timeout = 3000, expected = NoWorkerException.class)
  public void user_getWorkerReturnsWhenNotLocal() throws Exception {
    // this test was created to show that a infi loop happens
    // the timeout will protect against this, and the change was to throw a IOException
    // in the cases we don't want to disconnect from master
    MasterClient masterClient =
        new MasterClient(mMasterInfo.getMasterAddress(), mExecutorService, mMasterTachyonConf);
    masterClient.user_getWorker(false, "host.doesnotexist.fail");
    masterClient.close();
  }
}
