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

package alluxio.web;

import alluxio.Configuration;
import alluxio.LocalAlluxioClusterResource;
import alluxio.master.LocalAlluxioCluster;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Map.Entry;
import java.util.Scanner;

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
        if (line.equals("<title>Alluxio</title>") || line.equals("<title>Workers</title>")) {
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
