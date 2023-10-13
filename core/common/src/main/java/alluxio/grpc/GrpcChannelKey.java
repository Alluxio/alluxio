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

import com.google.common.base.MoreObjects;

import java.util.Objects;
import java.util.UUID;

/**
 * Used to define a key for {@link GrpcChannelPool}.
 */
public class GrpcChannelKey {
  private final GrpcNetworkGroup mNetworkGroup;
  private final GrpcServerAddress mServerAddress;

  /** Unique channel identifier. */
  private final UUID mChannelId = UUID.randomUUID();
  private final int mGroupIndex;

  /**
   * Constructor.
   * @param networkGroup network group
   * @param serverAddress server address
   * @param groupIndex the order of channel in slots that are allocated for its network group
   */
  protected GrpcChannelKey(GrpcNetworkGroup networkGroup,
      GrpcServerAddress serverAddress, int groupIndex) {
    mNetworkGroup = Objects.requireNonNull(networkGroup, "networkGroup is null");
    mServerAddress = Objects.requireNonNull(serverAddress, "serverAddress is null");
    mGroupIndex = groupIndex;
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
   * @return the network group
   */
  public GrpcNetworkGroup getNetworkGroup() {
    return mNetworkGroup;
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
        && mServerAddress.equals(other.mServerAddress)
        && mGroupIndex == other.mGroupIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(mNetworkGroup, mServerAddress, mGroupIndex);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("NetworkGroup", mNetworkGroup)
        .add("ServerAddress", mServerAddress)
        .add("GroupIndex", mGroupIndex)
        .add("ChannelId", mChannelId)
        .toString();
  }
}
