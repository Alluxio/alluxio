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
import alluxio.master.journal.JournalUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

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
    assertEquals(Sets.newHashSet(InetSocketAddress.createUnresolved("host1", 5),
        InetSocketAddress.createUnresolved("host2", 5),
        InetSocketAddress.createUnresolved("host3", 5)),
        new HashSet<>(conf.getClusterAddresses()));
  }

  @Test
  public void derivedJobMasterAddressesFromJobMasterAddresses() {
    ServerConfiguration.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES,
        "host1:10,host2:20,host3:10");
    ServerConfiguration.set(PropertyKey.JOB_MASTER_HOSTNAME, "host1");
    ServerConfiguration.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_PORT, 10);
    RaftJournalConfiguration conf = getConf(ServiceType.JOB_MASTER_RAFT);
    assertEquals(Sets.newHashSet(InetSocketAddress.createUnresolved("host1", 10),
        InetSocketAddress.createUnresolved("host2", 20),
        InetSocketAddress.createUnresolved("host3", 10)),
        new HashSet<>(conf.getClusterAddresses()));
  }

  @Test
  public void checkConfWhenLocalHostNameDiffWithRaftNodesHostName() {
    List<InetSocketAddress> clusterAddresses = new ArrayList<>();
    // Construct localAddress, its hostname is null and local ip is default 0.0.0.0.
    InetSocketAddress localAddress = new InetSocketAddress(10);
    // Construct raftNodeAddress1, its hostname derived from localHostName, So
    // raftNodeAddress1's hostname is different with localAddress hostname, but
    // their ip both point to the local node.
    InetSocketAddress raftNodeAddress1 = new InetSocketAddress(NetworkAddressUtils
        .getLocalHostName((int) ServerConfiguration.global()
        .getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)), 10);
    InetSocketAddress raftNodeAddress2 = new InetSocketAddress("host2", 20);
    InetSocketAddress raftNodeAddress3 = new InetSocketAddress("host3", 30);
    clusterAddresses.add(raftNodeAddress1);
    clusterAddresses.add(raftNodeAddress2);
    clusterAddresses.add(raftNodeAddress3);
    RaftJournalConfiguration conf = new RaftJournalConfiguration()
        .setClusterAddresses(clusterAddresses)
        .setElectionMinTimeoutMs(
            ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT))
        .setElectionMaxTimeoutMs(
            ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT))
        .setLocalAddress(localAddress)
        .setMaxLogSize(ServerConfiguration.getBytes(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX))
        .setPath(new File(JournalUtils.getJournalLocation().getPath()));
    conf.validate();
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
