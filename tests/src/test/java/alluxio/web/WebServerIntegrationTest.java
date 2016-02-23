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

package alluxio.web;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Map.Entry;
import java.util.Scanner;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

import alluxio.Configuration;
import alluxio.LocalAlluxioClusterResource;
import alluxio.master.LocalAlluxioCluster;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

/**
 * Tests the web server is up when Alluxio starts.
 */
public class WebServerIntegrationTest {
  private Configuration mMasterConf;
  private Configuration mWorkerConf;

  // Web pages that will be verified.
  private static final Multimap<ServiceType, String> PAGES =
      new ImmutableListMultimap.Builder<ServiceType, String>()
          .putAll(ServiceType.MASTER_WEB, "/home", "/browse", "/configuration", "/workers",
              "/memory", "/browseLogs", "/metricsui")
          .putAll(ServiceType.WORKER_WEB, "/home", "/blockInfo", "/browseLogs", "/metricsui")
          .build();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource();

  @Before
  public final void before() throws Exception {
    LocalAlluxioCluster localAlluxioCluster = mLocalAlluxioClusterResource.get();
    mMasterConf = localAlluxioCluster.getMasterConf();
    mWorkerConf = localAlluxioCluster.getWorkerConf();
  }

  private void verifyWebService(ServiceType serviceType, String path)
      throws IOException {
    Configuration conf =
        serviceType == ServiceType.MASTER_WEB ? mMasterConf : mWorkerConf;

    InetSocketAddress webAddr =
        NetworkAddressUtils.getConnectAddress(serviceType, conf);
    HttpURLConnection webService = (HttpURLConnection) new URL(
        "http://" + webAddr.getAddress().getHostAddress() + ":"
        + webAddr.getPort() + path).openConnection();
    webService.connect();
    Assert.assertEquals(200, webService.getResponseCode());

    Scanner pageScanner = null;
    boolean verified = false;

    try {
      pageScanner = new Scanner(webService.getInputStream());

      while (pageScanner.hasNextLine()) {
        String line = pageScanner.nextLine();
        if (line.equals("<title>Tachyon</title>") || line.equals("<title>Workers</title>")) {
          verified = true;
          break;
        }
      }
    } finally {
      if (pageScanner != null) {
        pageScanner.close();
      }
      webService.disconnect();
    }

    Assert.assertTrue(String.format("%s was started but not successfully verified.",
        serviceType.getServiceName()), verified);
  }

  /**
   * Tests whether the master and worker web homepage is up.
   */
  @Test
  public void serverUpTest() throws Exception {
    for (Entry<ServiceType, String> entry : PAGES.entries()) {
      verifyWebService(entry.getKey(), entry.getValue());
    }
  }
}
