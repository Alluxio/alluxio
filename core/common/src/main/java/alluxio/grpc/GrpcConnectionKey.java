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
public class GrpcConnectionKey {
  private GrpcChannelKey mChannelKey;
  private int mGroupIndex;

  /**
   * Creates a new gRPC connection key.
   * Connection-ID key format is:
   * - {@link GrpcChannelKey}
   * - Group Index:
   *   The order of channel in slots that are allocated for its network group.
   *
   * @param channelKey the gRPC channel key
   * @param groupIndex the group index
   */
  protected GrpcConnectionKey(GrpcChannelKey channelKey, int groupIndex) {
    mChannelKey = channelKey;
    mGroupIndex = groupIndex;
  }

  /**
   * @return the owner channel key
   */
  public GrpcChannelKey getChannelKey() {
    return mChannelKey;
  }

  /**
   * @return the id within group
   */
  public int getGroupIndex() {
    return mGroupIndex;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof GrpcConnectionKey)) {
      return false;
    }

    GrpcConnectionKey otherKey = (GrpcConnectionKey) other;
    return Objects.equals(mChannelKey, otherKey.mChannelKey)
        && Objects.equals(mGroupIndex, otherKey.mGroupIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mChannelKey, mGroupIndex);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ChannelKey", mChannelKey)
        .add("GroupIndex", mGroupIndex)
        .omitNullValues()
        .toString();
  }
}
