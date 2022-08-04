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
import static org.junit.Assert.assertTrue;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Units tests for {@link RaftJournalSystem}'s initialization.
 */
public final class RaftJournalSystemConfigTest {

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @After
  public void after() {
    Configuration.reloadProperties();
  }

  @Test
  public void defaultDefaults() throws Exception {
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "testhost");
    RaftJournalSystem system =
        new RaftJournalSystem(mFolder.newFolder().toURI(), ServiceType.MASTER_RAFT);
    InetSocketAddress address = getLocalAddress(system);
    assertEquals("testhost:" + 19200, address.toString());
  }

  @Test
  public void port() throws Exception {
    int testPort = 10000;
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT, testPort);
    RaftJournalSystem system =
        new RaftJournalSystem(mFolder.newFolder().toURI(), ServiceType.MASTER_RAFT);
    InetSocketAddress localAddress = getLocalAddress(system);
    List<InetSocketAddress> clusterAddresses = getClusterAddresses(system);
    assertEquals(testPort, localAddress.getPort());
    assertEquals(testPort, clusterAddresses.get(0).getPort());
  }

  @Test
  public void derivedMasterHostname() throws Exception {
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "test");
    RaftJournalSystem system =
        new RaftJournalSystem(mFolder.newFolder().toURI(), ServiceType.MASTER_RAFT);
    InetSocketAddress address = getLocalAddress(system);
    assertEquals("test:" + 19200, address.toString());
  }

  @Test
  public void derivedJobMasterHostname() throws Exception {
    Configuration.set(PropertyKey.JOB_MASTER_HOSTNAME, "test");
    RaftJournalSystem system =
        new RaftJournalSystem(mFolder.newFolder().toURI(), ServiceType.JOB_MASTER_RAFT);
    InetSocketAddress address = getLocalAddress(system);
    assertEquals("test:" + 20003, address.toString());
  }

  @Test
  public void derivedJobMasterHostnameFromMasterHostname() throws Exception {
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "test");
    RaftJournalSystem system =
        new RaftJournalSystem(mFolder.newFolder().toURI(), ServiceType.JOB_MASTER_RAFT);
    InetSocketAddress address = getLocalAddress(system);
    assertEquals("test:" + 20003, address.toString());
  }

  @Test
  public void derivedJobMasterAddressesFromMasterAddresses() throws Exception {
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES,
        "host1:10,host2:20,host3:10");
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "host1");
    Configuration.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_PORT, 5);
    RaftJournalSystem system =
        new RaftJournalSystem(mFolder.newFolder().toURI(), ServiceType.JOB_MASTER_RAFT);
    List<InetSocketAddress> addresses = getClusterAddresses(system);
    assertEquals(Sets.newHashSet(InetSocketAddress.createUnresolved("host1", 5),
            InetSocketAddress.createUnresolved("host2", 5),
            InetSocketAddress.createUnresolved("host3", 5)),
        new HashSet<>(addresses));
  }

  @Test
  public void derivedJobMasterAddressesFromJobMasterAddresses() throws Exception {
    Configuration.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES,
        "host1:10,host2:20,host3:10");
    Configuration.set(PropertyKey.JOB_MASTER_HOSTNAME, "host1");
    Configuration.set(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_PORT, 10);
    RaftJournalSystem system =
        new RaftJournalSystem(mFolder.newFolder().toURI(), ServiceType.JOB_MASTER_RAFT);
    List<InetSocketAddress> addresses = getClusterAddresses(system);
    assertEquals(Sets.newHashSet(InetSocketAddress.createUnresolved("host1", 10),
            InetSocketAddress.createUnresolved("host2", 20),
            InetSocketAddress.createUnresolved("host3", 10)),
        new HashSet<>(addresses));
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
        .getLocalHostName((int) Configuration.global()
            .getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)), 10);
    InetSocketAddress raftNodeAddress2 = new InetSocketAddress("host2", 20);
    InetSocketAddress raftNodeAddress3 = new InetSocketAddress("host3", 30);
    clusterAddresses.add(raftNodeAddress1);
    clusterAddresses.add(raftNodeAddress2);
    clusterAddresses.add(raftNodeAddress3);
    assertTrue(clusterAddresses.contains(localAddress)
        || NetworkAddressUtils.containsLocalIp(clusterAddresses, Configuration.global()));
  }

  private InetSocketAddress getLocalAddress(RaftJournalSystem system) throws Exception {
    Field field = RaftJournalSystem.class.getDeclaredField("mLocalAddress");
    field.setAccessible(true);
    return (InetSocketAddress) field.get(system);
  }

  @SuppressWarnings("unchecked")
  private List<InetSocketAddress> getClusterAddresses(RaftJournalSystem system) throws Exception {
    Field field = RaftJournalSystem.class.getDeclaredField("mClusterAddresses");
    field.setAccessible(true);
    return (List<InetSocketAddress>) field.get(system);
  }
}
