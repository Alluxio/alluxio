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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.JournalUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Configuration for the Raft journal system.
 */
public class RaftJournalConfiguration {
  private List<InetSocketAddress> mClusterAddresses;
  private long mMinElectionTimeoutMs;
  private long mMaxElectionTimeoutMs;
  private InetSocketAddress mLocalAddress;
  private long mMaxLogSize;
  private File mPath;
  private Integer mMaxConcurrencyPoolSize;

  /**
   * @param serviceType either master raft service or job master raft service
   * @return default configuration for the specified service type
   */
  public static RaftJournalConfiguration defaults(ServiceType serviceType) {
    return new RaftJournalConfiguration()
        .setClusterAddresses(ConfigurationUtils
            .getEmbeddedJournalAddresses(ServerConfiguration.global(), serviceType))
        .setElectionMinTimeoutMs(
            ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT))
        .setElectionMaxTimeoutMs(
            ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT))
        .setLocalAddress(NetworkAddressUtils.getConnectAddress(serviceType,
            ServerConfiguration.global()))
        .setMaxLogSize(ServerConfiguration.getBytes(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX))
        .setPath(new File(JournalUtils.getJournalLocation().getPath()))
        .setMaxConcurrencyPoolSize(
            ServerConfiguration.getInt(PropertyKey.MASTER_JOURNAL_LOG_CONCURRENCY_MAX));
  }

  /**
   * Validates the configuration.
   */
  public void validate() {
    Preconditions.checkState(getMaxLogSize() <= Integer.MAX_VALUE,
        "{} has value {} but must not exceed {}", PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX,
        getMaxLogSize(), Integer.MAX_VALUE);
    Preconditions.checkState(getClusterAddresses().contains(getLocalAddress())
            || NetworkAddressUtils.containsLocalIp(getClusterAddresses(),
        ServerConfiguration.global()),
        "The cluster addresses (%s) must contain the local master address (%s)",
        getClusterAddresses(), getLocalAddress());
    Preconditions.checkState(getMinElectionTimeoutMs() < getMaxElectionTimeoutMs(),
        "min election timeout (%sms) should be less than max election timeout (%sms)",
        getMinElectionTimeoutMs(), getMaxElectionTimeoutMs());
  }

  /**
   * @return addresses of all nodes in the Raft cluster
   */
  public List<InetSocketAddress> getClusterAddresses() {
    return mClusterAddresses;
  }

  /**
   * @return min election timeout
   */
  public long getMinElectionTimeoutMs() {
    return mMinElectionTimeoutMs;
  }

  /**
   * @return max election timeout
   */
  public long getMaxElectionTimeoutMs() {
    return mMaxElectionTimeoutMs;
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
   * @param clusterAddresses addresses of all nodes in the Raft cluster
   * @return the updated configuration
   */
  public RaftJournalConfiguration setClusterAddresses(List<InetSocketAddress> clusterAddresses) {
    mClusterAddresses = clusterAddresses;
    return this;
  }

  /**
   * @param minElectionTimeoutMs min election timeout
   * @return the updated configuration
   */
  public RaftJournalConfiguration setElectionMinTimeoutMs(long minElectionTimeoutMs) {
    mMinElectionTimeoutMs = minElectionTimeoutMs;
    return this;
  }

  /**
   * @param maxElectionTimeoutMs max election timeout
   * @return the updated configuration
   */
  public RaftJournalConfiguration setElectionMaxTimeoutMs(long maxElectionTimeoutMs) {
    mMaxElectionTimeoutMs = maxElectionTimeoutMs;
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
   * @return the maxConcurrencyPoolSize
   */
  public Integer getMaxConcurrencyPoolSize() {
    return mMaxConcurrencyPoolSize;
  }

  /**
   * @param maxConcurrencyPoolSize max thread size for notifyTermIndexUpdated method
   * @return the updated configuration
   */
  public RaftJournalConfiguration setMaxConcurrencyPoolSize(Integer maxConcurrencyPoolSize) {
    mMaxConcurrencyPoolSize = maxConcurrencyPoolSize;
    return this;
  }
}
