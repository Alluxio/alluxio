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

package alluxio.util;

import static org.junit.Assert.assertEquals;

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Unit tests for {@link ConfigurationUtils}.
 */
public final class ConfigurationUtilsTest {
  @Test
  public void getSingleMasterRpcAddress() {
    AlluxioConfiguration conf = createConf(ImmutableMap.of(
        PropertyKey.MASTER_HOSTNAME, "testhost",
        PropertyKey.MASTER_RPC_PORT, 1000));
    assertEquals(Arrays.asList(InetSocketAddress.createUnresolved("testhost", 1000)),
        ConfigurationUtils.getMasterRpcAddresses(conf));
  }

  @Test
  public void getMasterRpcAddresses() {
    AlluxioConfiguration conf =
        createConf(ImmutableMap.of(PropertyKey.MASTER_RPC_ADDRESSES, "host1:99,host2:100"));
    assertEquals(
        Arrays.asList(InetSocketAddress.createUnresolved("host1", 99),
            InetSocketAddress.createUnresolved("host2", 100)),
        ConfigurationUtils.getMasterRpcAddresses(conf));
  }

  @Test
  public void getMasterRpcAddressesFallback() {
    AlluxioConfiguration conf =
        createConf(ImmutableMap.of(
            PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES, "host1:99,host2:100",
            PropertyKey.MASTER_RPC_PORT, 50));
    assertEquals(
        Arrays.asList(InetSocketAddress.createUnresolved("host1", 50),
            InetSocketAddress.createUnresolved("host2", 50)),
        ConfigurationUtils.getMasterRpcAddresses(conf));
  }

  @Test
  public void getMasterRpcAddressesDefault() {
    AlluxioConfiguration conf = createConf(Collections.emptyMap());
    String host = NetworkAddressUtils.getLocalHostName(5 * Constants.SECOND_MS);
    assertEquals(Arrays.asList(InetSocketAddress.createUnresolved(host, 19998)),
        ConfigurationUtils.getMasterRpcAddresses(conf));
  }

  @Test
  public void getSingleJobMasterRpcAddress() {
    AlluxioConfiguration conf = createConf(ImmutableMap.of(
        PropertyKey.JOB_MASTER_HOSTNAME, "testhost",
        PropertyKey.JOB_MASTER_RPC_PORT, 1000));
    assertEquals(Arrays.asList(InetSocketAddress.createUnresolved("testhost", 1000)),
        ConfigurationUtils.getJobMasterRpcAddresses(conf));
  }

  @Test
  public void getJobMasterRpcAddresses() {
    AlluxioConfiguration conf =
        createConf(ImmutableMap.of(PropertyKey.JOB_MASTER_RPC_ADDRESSES, "host1:99,host2:100"));
    assertEquals(
        Arrays.asList(InetSocketAddress.createUnresolved("host1", 99),
            InetSocketAddress.createUnresolved("host2", 100)),
        ConfigurationUtils.getJobMasterRpcAddresses(conf));
  }

  @Test
  public void getJobMasterRpcAddressesMasterRpcFallback() {
    AlluxioConfiguration conf =
        createConf(ImmutableMap.of(
            PropertyKey.MASTER_RPC_ADDRESSES, "host1:99,host2:100",
            PropertyKey.JOB_MASTER_RPC_PORT, 50));
    assertEquals(
        Arrays.asList(InetSocketAddress.createUnresolved("host1", 50),
            InetSocketAddress.createUnresolved("host2", 50)),
        ConfigurationUtils.getJobMasterRpcAddresses(conf));
  }

  @Test
  public void getJobMasterRpcAddressesServerFallback() {
    AlluxioConfiguration conf =
        createConf(ImmutableMap.of(
            PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES, "host1:99,host2:100",
            PropertyKey.JOB_MASTER_RPC_PORT, 50));
    assertEquals(
        Arrays.asList(InetSocketAddress.createUnresolved("host1", 50),
            InetSocketAddress.createUnresolved("host2", 50)),
        ConfigurationUtils.getJobMasterRpcAddresses(conf));
  }

  @Test
  public void getJobMasterRpcAddressesDefault() {
    AlluxioConfiguration conf = createConf(Collections.emptyMap());
    String host = NetworkAddressUtils.getLocalHostName(5 * Constants.SECOND_MS);
    assertEquals(Arrays.asList(InetSocketAddress.createUnresolved(host, 20001)),
        ConfigurationUtils.getJobMasterRpcAddresses(conf));
  }

  @Test
  public void parseAsList() {
    assertEquals(Lists.newArrayList("a"), ConfigurationUtils.parseAsList("a", ","));
    assertEquals(Lists.newArrayList("a", "b", "c"), ConfigurationUtils.parseAsList("a,b,c", ","));
    assertEquals(Lists.newArrayList("a", "b", "c"),
        ConfigurationUtils.parseAsList(" a , b , c ", ","));
    assertEquals(Lists.newArrayList("a,b,c"), ConfigurationUtils.parseAsList("a,b,c", ";"));
    assertEquals(Lists.newArrayList("a", "c"), ConfigurationUtils.parseAsList(",,a,,c,,", ","));
  }

  private AlluxioConfiguration createConf(Map<PropertyKey, Object> properties) {
    AlluxioProperties props = Configuration.copyProperties();
    for (PropertyKey key : properties.keySet()) {
      props.set(key, properties.get(key));
    }
    return new InstancedConfiguration(props);
  }
}
