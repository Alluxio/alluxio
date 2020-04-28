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

/**
 * Used to define a key for {@link GrpcConnectionPool}.
 */
public class GrpcResourceKey {
  private GrpcChannelKey mChannelKey;
  private ManagedChannelKey mManagedChannelKey;
  private EventLoopGroupKey mEventLoopGroupKey;

  /**
   * Creates a new gRPC connection key. Connection-ID key format is: - {@link GrpcChannelKey} -
   * Group Index: Order within slots that are allocated to connections for the network group. -
   * EventLoop Group Index: Order within slots that are allocated to event-loop groups for the
   * network group.
   *
   * @param channelKey the gRPC channel key
   * @param managedChannelIndex the managed-channel index
   * @param eventLoopGroupIndex the event-loop group index
   */
  protected GrpcResourceKey(GrpcChannelKey channelKey, int managedChannelIndex,
      int eventLoopGroupIndex) {
    mChannelKey = channelKey;
    mManagedChannelKey = new ManagedChannelKey(channelKey.getNetworkGroup(),
        channelKey.getServerAddress(), managedChannelIndex);
    mEventLoopGroupKey = new EventLoopGroupKey(channelKey.getNetworkGroup(), eventLoopGroupIndex);
  }

  /**
   * @return the client channel key
   */
  public GrpcChannelKey getClientChannelKey() {
    return mChannelKey;
  }

  /**
   * @return the internal managed-channel key
   */
  public ManagedChannelKey getManagedChannelKey() {
    return mManagedChannelKey;
  }

  /**
   * @return the internal event-loop-group key
   */
  public EventLoopGroupKey getEventLoopGroupKey() {
    return mEventLoopGroupKey;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("ChannelKey", mChannelKey)
        .add("ManagedChannelKey", mManagedChannelKey).add("EventLoopGroupKey", mEventLoopGroupKey)
        .omitNullValues().toString();
  }

  class ManagedChannelKey {
    private GrpcChannelKey.NetworkGroup mNetworkGroup;
    private GrpcServerAddress mServerAddress;
    private int mIndex;

    protected ManagedChannelKey(GrpcChannelKey.NetworkGroup networkGroup,
        GrpcServerAddress serverAddress, int index) {
      mNetworkGroup = networkGroup;
      mServerAddress = serverAddress;
      mIndex = index;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof ManagedChannelKey)) {
        return false;
      }

      ManagedChannelKey otherKey = (ManagedChannelKey) other;
      return Objects.equals(mNetworkGroup, otherKey.mNetworkGroup)
          && Objects.equals(mServerAddress, otherKey.mServerAddress)
          && Objects.equals(mIndex, otherKey.mIndex);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mNetworkGroup, mServerAddress, mIndex);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("NetworkGroup", mNetworkGroup)
          .add("ServerAddress", mServerAddress).add("ConnectionIndex", mIndex).omitNullValues()
          .toString();
    }
  }

  class EventLoopGroupKey {
    private GrpcChannelKey.NetworkGroup mNetworkGroup;
    private int mIndex;

    protected EventLoopGroupKey(GrpcChannelKey.NetworkGroup networkGroup, int index) {
      mNetworkGroup = networkGroup;
      mIndex = index;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof EventLoopGroupKey)) {
        return false;
      }

      EventLoopGroupKey otherKey = (EventLoopGroupKey) other;
      return Objects.equals(mNetworkGroup, otherKey.mNetworkGroup)
          && Objects.equals(mIndex, otherKey.mIndex);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mNetworkGroup, mIndex);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("NetworkGroup", mNetworkGroup)
          .add("Index", mIndex).omitNullValues().toString();
    }
  }
}
