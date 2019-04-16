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

package alluxio.master.journal.raft;

import static org.junit.Assert.assertEquals;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashSet;

/**
 * Units tests for {@link RaftJournalConfiguration}.
 */
public final class RaftJournalConfigurationTest {
  @Before
  public void before() {
    ServerConfiguration.set(PropertyKey.MASTER_HOSTNAME, "testhost");
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void defaultDefaults() {
    RaftJournalConfiguration conf = getConf(ServiceType.MASTER_RAFT);
    checkAddress("testhost", 19200, conf.getLocalAddress());
  }

  @Test
  public void port() {
    int testPort = 10000;
    ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT, testPort);
    RaftJournalConfiguration conf = getConf(ServiceType.MASTER_RAFT);
    assertEquals(testPort, conf.getLocalAddress().getPort());
    assertEquals(testPort, conf.getClusterAddresses().get(0).getPort());
  }

  @Test
  public void derivedMasterHostname() {
    ServerConfiguration.set(PropertyKey.MASTER_HOSTNAME, "test");
    RaftJournalConfiguration conf = getConf(ServiceType.MASTER_RAFT);
    checkAddress("test", 19200, conf.getLocalAddress());
  }

  @Test
  public void derivedJobMasterHostname() {
    ServerConfiguration.set(PropertyKey.JOB_MASTER_HOSTNAME, "test");
    RaftJournalConfiguration conf = getConf(ServiceType.JOB_MASTER_RAFT);
    checkAddress("test", 20003, conf.getLocalAddress());
  }

  @Test
  public void derivedJobMasterHostnameFromMasterHostname() {
    ServerConfiguration.set(PropertyKey.MASTER_HOSTNAME, "test");
    RaftJournalConfiguration conf = getConf(ServiceType.JOB_MASTER_RAFT);
    checkAddress("test", 20003, conf.getLocalAddress());
  }

  @Test
  public void derivedJobMasterAddressesFromMasterAddresses() {
    ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES,
        "host1:10,host2:20,host3:10");
    ServerConfiguration.set(PropertyKey.MASTER_HOSTNAME, "host1");
    ServerConfiguration.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_PORT, 5);
    RaftJournalConfiguration conf = getConf(ServiceType.JOB_MASTER_RAFT);
    assertEquals(Sets.newHashSet(new InetSocketAddress("host1", 5),
        new InetSocketAddress("host2", 5),
        new InetSocketAddress("host3", 5)),
        new HashSet<>(conf.getClusterAddresses()));
  }

  @Test
  public void derivedJobMasterAddressesFromJobMasterAddresses() {
    ServerConfiguration.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES,
        "host1:10,host2:20,host3:10");
    ServerConfiguration.set(PropertyKey.JOB_MASTER_HOSTNAME, "host1");
    ServerConfiguration.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_PORT, 10);
    RaftJournalConfiguration conf = getConf(ServiceType.JOB_MASTER_RAFT);
    assertEquals(Sets.newHashSet(new InetSocketAddress("host1", 10),
        new InetSocketAddress("host2", 20),
        new InetSocketAddress("host3", 10)),
        new HashSet<>(conf.getClusterAddresses()));
  }

  private RaftJournalConfiguration getConf(ServiceType serviceType) {
    RaftJournalConfiguration conf = RaftJournalConfiguration.defaults(serviceType);
    conf.validate();
    return conf;
  }

  private void checkAddress(String hostname, int port, InetSocketAddress address) {
    assertEquals(hostname, address.getHostString());
    assertEquals(port, address.getPort());
  }
}
