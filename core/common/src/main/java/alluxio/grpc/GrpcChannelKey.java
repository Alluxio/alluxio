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

package alluxio.grpc;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.MoreObjects;

import java.util.Objects;
import java.util.UUID;

/**
 * Used to identify a unique {@link GrpcChannel}.
 */
public class GrpcChannelKey {
  GrpcNetworkGroup mNetworkGroup = GrpcNetworkGroup.RPC;
  private GrpcServerAddress mServerAddress;

  /** Unique channel identifier. */
  private final UUID mChannelId = UUID.randomUUID();
  /** Hostname to send to server for identification. */
  private final String mLocalHostName;

  /** Client that requires a channel. */
  private String mClientType;

  private GrpcChannelKey(AlluxioConfiguration conf) {
    // Try to get local host name.
    String localHostName;
    try {
      localHostName = NetworkAddressUtils
          .getLocalHostName((int) conf.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    } catch (Exception e) {
      localHostName = NetworkAddressUtils.UNKNOWN_HOSTNAME;
    }
    mLocalHostName = localHostName;
  }

  /**
   * Creates a {@link GrpcChannelKey}.
   *
   * @param conf the Alluxio configuration
   * @return the created instance
   */
  public static GrpcChannelKey create(AlluxioConfiguration conf) {
    return new GrpcChannelKey(conf);
  }

  /**
   * @return unique identifier for the channel
   */
  public UUID getChannelId() {
    return mChannelId;
  }

  /**
   * @return destination address of the channel
   */
  public GrpcServerAddress getServerAddress() {
    return mServerAddress;
  }

  /**
   * @param address destination address of the channel
   * @return the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setServerAddress(GrpcServerAddress address) {
    mServerAddress = address;
    return this;
  }

  /**
   * @param group the networking group membership
   * @return the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setNetworkGroup(GrpcNetworkGroup group) {
    mNetworkGroup = group;
    return this;
  }

  /**
   * @return the network group
   */
  public GrpcNetworkGroup getNetworkGroup() {
    return mNetworkGroup;
  }

  /**
   * @param clientType the client type
   * @return the modified {@link GrpcChannelKey}
   */
  public GrpcChannelKey setClientType(String clientType) {
    mClientType = clientType;
    return this;
  }

  @Override
  public int hashCode() {
    return Objects.hash(mNetworkGroup, mServerAddress);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GrpcChannelKey other = (GrpcChannelKey) o;
    return mNetworkGroup.equals(other.mNetworkGroup)
        && mServerAddress.equals(other.mServerAddress);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ClientType", mClientType)
        .add("ClientHostname", mLocalHostName)
        .add("ServerAddress", mServerAddress)
        .add("ChannelId", mChannelId)
        .omitNullValues()
        .toString();
  }
}
