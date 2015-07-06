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

package tachyon.worker.block;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.NetworkUtils;

public class BlockWorkerUtilsTest {
  private static Map<String, String> sTestProperties = new LinkedHashMap<String, String>();
  private static Map<String, String> sNullMasterHostNameTestProperties =
      new LinkedHashMap<String, String>();
  private static Map<String, String> sNullMasterPortTestProperties =
      new LinkedHashMap<String, String>();
  private static Map<String, String> sNullTestProperties =
      new LinkedHashMap<String, String>();

  private TachyonConf mCustomPropsTachyonConf;
  private TachyonConf mNullMasterHostNameTachyonConf;
  private TachyonConf mNullMasterPortTachyonConf;
  private TachyonConf mNullTachyonConf;

  @BeforeClass
  public static void beforeClass() {
    // initialize the test properties.
    sTestProperties.put(Constants.MASTER_HOSTNAME, "RemoteMaster1");
    sTestProperties.put(Constants.MASTER_PORT, "10000");
    sTestProperties.put(Constants.WORKER_PORT, "10001");

    // initialize the null master host name test properties.
    sNullMasterHostNameTestProperties.put(Constants.MASTER_PORT, "20000");

    // initialize the null master port test properties.
    sNullMasterPortTestProperties.put(Constants.MASTER_HOSTNAME, "RemoteMaster3");
  }

  @Before
  public void before() {
    // init TachyonConf
    mCustomPropsTachyonConf = new TachyonConf(sTestProperties);
    mNullMasterHostNameTachyonConf = new TachyonConf(sNullMasterHostNameTestProperties);
    mNullMasterPortTachyonConf = new TachyonConf(sNullMasterPortTestProperties);
    mNullTachyonConf = new TachyonConf(sNullTestProperties);
  }

  @Test
  public void getMasterAddressTest() {
    String defaultHostname = NetworkUtils.getLocalHostName(mCustomPropsTachyonConf);
    int defaultPort = Constants.DEFAULT_MASTER_PORT;

    InetSocketAddress masterAddress = BlockWorkerUtils.getMasterAddress(mCustomPropsTachyonConf);
    Assert.assertNotNull(masterAddress);
    Assert.assertEquals(new InetSocketAddress("RemoteMaster1", 10000), masterAddress);

    masterAddress = BlockWorkerUtils.getMasterAddress(mNullMasterHostNameTachyonConf);
    Assert.assertNotNull(masterAddress);
    Assert.assertEquals(new InetSocketAddress(defaultHostname, 20000), masterAddress);

    masterAddress = BlockWorkerUtils.getMasterAddress(mNullMasterPortTachyonConf);
    Assert.assertNotNull(masterAddress);
    Assert.assertEquals(new InetSocketAddress("RemoteMaster3", defaultPort), masterAddress);

    masterAddress = BlockWorkerUtils.getMasterAddress(mNullTachyonConf);
    Assert.assertNotNull(masterAddress);
    Assert.assertEquals(new InetSocketAddress(defaultHostname, defaultPort), masterAddress);
  }

  @Test
  public void getWorkerAddressTest() {
    String defaultHostname = NetworkUtils.getLocalHostName(mCustomPropsTachyonConf);
    int defaultPort = Constants.DEFAULT_WORKER_PORT;

    InetSocketAddress workerAddress = BlockWorkerUtils.getWorkerAddress(mCustomPropsTachyonConf);
    Assert.assertNotNull(workerAddress);
    Assert.assertEquals(new InetSocketAddress(defaultHostname, 10001), workerAddress);

    workerAddress = BlockWorkerUtils.getWorkerAddress(mNullTachyonConf);
    Assert.assertNotNull(workerAddress);
    Assert.assertEquals(new InetSocketAddress(defaultHostname, defaultPort), workerAddress);
  }
}
