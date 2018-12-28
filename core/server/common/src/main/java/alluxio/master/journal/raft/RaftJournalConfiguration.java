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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.journal.JournalUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for the Raft journal system.
 */
public class RaftJournalConfiguration {
  private List<InetSocketAddress> mClusterAddresses;
  private long mElectionTimeoutMs;
  private long mHeartbeatIntervalMs;
  private InetSocketAddress mLocalAddress;
  private long mMaxLogSize;
  private File mPath;
  private StorageLevel mStorageLevel;

  /**
   * Enum corresponding to io.atomix.copycat.server.storage.StorageLevel. We cannot use that class
   * here because the atomix module requires Java 8.
   */
  public enum StorageLevel {
    DISK, MAPPED, MEMORY;
  }

  /**
   * @param serviceType either master raft service or job master raft service
   * @return default configuration for the specified service type
   */
  public static RaftJournalConfiguration defaults(ServiceType serviceType) {
    return new RaftJournalConfiguration()
        .setClusterAddresses(defaultClusterAddresses(serviceType))
        .setElectionTimeoutMs(
            Configuration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT))
        .setHeartbeatIntervalMs(
            Configuration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_HEARTBEAT_INTERVAL))
        .setLocalAddress(NetworkAddressUtils.getConnectAddress(serviceType))
        .setMaxLogSize(Configuration.getBytes(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX))
        .setPath(new File(JournalUtils.getJournalLocation().getPath()))
        .setStorageLevel(Configuration.getEnum(PropertyKey.MASTER_EMBEDDED_JOURNAL_STORAGE_LEVEL,
            StorageLevel.class));
  }

  /**
   * @return addresses of all nodes in the Raft cluster
   */
  public List<InetSocketAddress> getClusterAddresses() {
    return mClusterAddresses;
  }

  /**
   * @return election timeout
   */
  public long getElectionTimeoutMs() {
    return mElectionTimeoutMs;
  }

  /**
   * @return heartbeat interval
   */
  public long getHeartbeatIntervalMs() {
    return mHeartbeatIntervalMs;
  }

  /**
   * @return address of this Raft cluster node
   */
  public InetSocketAddress getLocalAddress() {
    return mLocalAddress;
  }

  /**
   * @return max log file size
   */
  public long getMaxLogSize() {
    return mMaxLogSize;
  }

  /**
   * @return where to store journal logs
   */
  public File getPath() {
    return mPath;
  }

  /**
   * @return storage level
   */
  public StorageLevel getStorageLevel() {
    return mStorageLevel;
  }

  /**
   * @param clusterAddresses addresses of all nodes in the Raft cluster
   * @return the updated configuration
   */
  public RaftJournalConfiguration setClusterAddresses(List<InetSocketAddress> clusterAddresses) {
    mClusterAddresses = clusterAddresses;
    return this;
  }

  /**
   * @param electionTimeoutMs election timeout
   * @return the updated configuration
   */
  public RaftJournalConfiguration setElectionTimeoutMs(long electionTimeoutMs) {
    mElectionTimeoutMs = electionTimeoutMs;
    return this;
  }

  /**
   * @param heartbeatIntervalMs heartbeat interval
   * @return the updated configuration
   */
  public RaftJournalConfiguration setHeartbeatIntervalMs(long heartbeatIntervalMs) {
    mHeartbeatIntervalMs = heartbeatIntervalMs;
    return this;
  }

  /**
   * @param localAddress address of this Raft cluster node
   * @return the updated configuration
   */
  public RaftJournalConfiguration setLocalAddress(InetSocketAddress localAddress) {
    mLocalAddress = localAddress;
    return this;
  }

  /**
   * @param maxLogSize maximum log file size
   * @return the updated configuration
   */
  public RaftJournalConfiguration setMaxLogSize(long maxLogSize) {
    mMaxLogSize = maxLogSize;
    return this;
  }

  /**
   * @param path where to store journal logs
   * @return the updated configuration
   */
  public RaftJournalConfiguration setPath(File path) {
    mPath = path;
    return this;
  }

  /**
   * @param storageLevel storage level
   * @return the updated configuration
   */
  public RaftJournalConfiguration setStorageLevel(StorageLevel storageLevel) {
    mStorageLevel = storageLevel;
    return this;
  }

  private static List<InetSocketAddress> defaultClusterAddresses(ServiceType serviceType) {
    PropertyKey addressKey;
    if (serviceType.equals(ServiceType.MASTER_RAFT)) {
      addressKey = PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES;
    } else {
      Preconditions.checkState(serviceType.equals(ServiceType.JOB_MASTER_RAFT));
      // If the job master embedded journal addresses aren't explicitly configured, default to
      // using the same hostnames as the alluxio master embedded journal addresses, but with the job
      // master port.
      if (!Configuration.containsKey(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES)) {
        List<InetSocketAddress> addrs = defaultClusterAddresses(ServiceType.MASTER_RAFT);
        List<InetSocketAddress> jobAddrs = new ArrayList<>(addrs.size());
        int port = NetworkAddressUtils.getPort(ServiceType.JOB_MASTER_RAFT);
        for (InetSocketAddress addr : addrs) {
          jobAddrs.add(new InetSocketAddress(addr.getHostName(), port));
        }
        return jobAddrs;
      }
      addressKey = PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES;
    }
    List<String> addresses = Configuration.getList(addressKey, ",");
    List<InetSocketAddress> inetAddresses = new ArrayList<>();
    for (String address : addresses) {
      try {
        inetAddresses.add(NetworkAddressUtils.parseInetSocketAddress(address));
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format("Failed to parse address %s for property %s", address, addressKey), e);
      }
    }
    return inetAddresses;
  }
}
