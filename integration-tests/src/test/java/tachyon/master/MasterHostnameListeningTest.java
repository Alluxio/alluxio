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
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
import tachyon.util.NetworkUtils;

/**
 * Simple tests for the MASTER_HOSTNAME_LISTENING configuration option.
 */
public class MasterHostnameListeningTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private MasterInfo mMasterInfo = null;
  private final ExecutorService mExecutorService = Executors.newFixedThreadPool(2);
  private TachyonConf mMasterTachyonConf = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    mExecutorService.shutdown();
  }

  private final void startCluster(String hostnameListening) throws IOException {
    mLocalTachyonCluster = new LocalTachyonCluster(100, 100, Constants.GB);
    TachyonConf tachyonConf = new TachyonConf();
    if (hostnameListening != null) {
      tachyonConf.set(Constants.MASTER_HOSTNAME_LISTENING, hostnameListening);
    }
    mLocalTachyonCluster.start(tachyonConf);
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
  }

  @Test
  public void listenEmptyTest() throws IOException {
    startCluster(null);
    MasterClient masterClient =
        new MasterClient(mMasterInfo.getMasterAddress(), mExecutorService, mMasterTachyonConf);
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.close();
  }

  @Test
  public void listenWildcardTest() throws IOException {
    startCluster("*");
    MasterClient masterClient =
        new MasterClient(mMasterInfo.getMasterAddress(), mExecutorService, mMasterTachyonConf);
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.close();
  }

  @Test
  public void listenSameAddressTest() throws IOException {
    startCluster(NetworkUtils.getLocalHostName(100));
    MasterClient masterClient =
        new MasterClient(mMasterInfo.getMasterAddress(), mExecutorService, mMasterTachyonConf);
    masterClient.connect();
    Assert.assertTrue(masterClient.isConnected());
    masterClient.close();
  }
}
