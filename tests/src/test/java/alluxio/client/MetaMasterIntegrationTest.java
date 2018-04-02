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

package alluxio.client;

import static org.junit.Assert.assertEquals;

import alluxio.BaseIntegrationTest;
import alluxio.LocalAlluxioClusterResource;
import alluxio.master.MasterClientConfig;
import alluxio.wire.ConfigInfo;
import alluxio.wire.MasterInfo;
import alluxio.wire.MasterInfo.MasterInfoField;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Integration tests for the meta master.
 */
public final class MetaMasterIntegrationTest extends BaseIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource.Builder().build();

  private int mWebPort;

  @Before
  public void prepareWebPort() throws Exception {
    mWebPort = mResource.get().getLocalAlluxioMaster().getMasterProcess().getWebAddress().getPort();
  }

  @Test
  public void getInfoAllFields() throws Exception {
    try (MetaMasterClient client =
        new RetryHandlingMetaMasterClient(MasterClientConfig.defaults())) {
      MasterInfo info = client.getMasterInfo(null);
      assertEquals(mWebPort, info.getWebPort());
    }
  }

  @Test
  public void getMasterInfoWebPort() throws Exception {
    try (MetaMasterClient client =
        new RetryHandlingMetaMasterClient(MasterClientConfig.defaults())) {
      MasterInfo info = client.getMasterInfo(new HashSet<>(Arrays
          .asList(MasterInfoField.WEB_PORT)));
      assertEquals(mWebPort, info.getWebPort());
    }
  }

  @Test
  public void getConfigInfoWebPort() throws Exception {
    try (MetaMasterClient client =
             new RetryHandlingMetaMasterClient(MasterClientConfig.defaults())) {
      List<ConfigInfo> configInfoList = client.getConfigInfoList();
      int configWebPort = -1;
      for (ConfigInfo info : configInfoList) {
        if (info.getName().equals("alluxio.master.web.port")) {
          configWebPort = Integer.valueOf(info.getValue());
        }
      }
      assertEquals(mWebPort, configWebPort);
    }
  }
}
