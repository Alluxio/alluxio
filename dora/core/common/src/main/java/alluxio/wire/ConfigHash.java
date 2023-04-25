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

package alluxio.wire;

import alluxio.grpc.GetConfigHashPResponse;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Hashes of cluster and path level configurations.
 */
@ThreadSafe
public class ConfigHash {
  private final String mClusterConfigHash;
  private final String mPathConfigHash;

  /**
   * Constructs a new ConfigHash.
   *
   * @param clusterConfigHash cluster configuration hash, cannot be null
   * @param pathConfigHash path configuration hash, cannot be null
   */
  public ConfigHash(String clusterConfigHash, String pathConfigHash) {
    Preconditions.checkNotNull(clusterConfigHash, "clusterConfigHash");
    Preconditions.checkNotNull(pathConfigHash, "pathConfigHash");
    mClusterConfigHash = clusterConfigHash;
    mPathConfigHash = pathConfigHash;
  }

  private ConfigHash(GetConfigHashPResponse response) {
    mClusterConfigHash = response.getClusterConfigHash();
    mPathConfigHash = response.getPathConfigHash();
  }

  /**
   * @param response the grpc representation of configuration hash
   * @return the wire representation of the proto response
   */
  public static ConfigHash fromProto(GetConfigHashPResponse response) {
    return new ConfigHash(response);
  }

  /**
   * @return the proto representation
   */
  public GetConfigHashPResponse toProto() {
    GetConfigHashPResponse.Builder response = GetConfigHashPResponse.newBuilder();
    if (mClusterConfigHash != null) {
      response.setClusterConfigHash(mClusterConfigHash);
    }
    if (mPathConfigHash != null) {
      response.setPathConfigHash(mPathConfigHash);
    }
    return response.build();
  }

  /**
   * @return hash of cluster level configuration
   */
  public String getClusterConfigHash() {
    return mClusterConfigHash;
  }

  /**
   * @return hash of path level configuration
   */
  public String getPathConfigHash() {
    return mPathConfigHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ConfigHash)) {
      return false;
    }
    ConfigHash that = (ConfigHash) o;
    return mClusterConfigHash.equals(that.mClusterConfigHash)
        && mPathConfigHash.equals(that.mPathConfigHash);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mClusterConfigHash, mPathConfigHash);
  }
}
