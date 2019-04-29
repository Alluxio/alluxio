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

import alluxio.grpc.GetConfigVersionPResponse;

import com.google.common.base.Objects;

/**
 * Version of cluster and path level configurations.
 */
public class ConfigVersion {
  private String mClusterConfigVersion;
  private String mPathConfigVersion;

  private ConfigVersion(GetConfigVersionPResponse response) {
    mClusterConfigVersion = response.getClusterConfigVersion();
    mPathConfigVersion = response.getPathConfigVersion();
  }

  /**
   * @param response the grpc representation of configuration version
   * @return the wire representation of the proto response
   */
  public static ConfigVersion fromProto(GetConfigVersionPResponse response) {
    return new ConfigVersion(response);
  }

  /**
   * @return version of cluster level configuration
   */
  public String getClusterConfigVersion() {
    return mClusterConfigVersion;
  }

  /**
   * @return version of path level configuration
   */
  public String getPathConfigVersion() {
    return mPathConfigVersion;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ConfigVersion)) {
      return false;
    }
    ConfigVersion that = (ConfigVersion) o;
    return mClusterConfigVersion.equals(that.mClusterConfigVersion)
        && mPathConfigVersion.equals(that.mPathConfigVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mClusterConfigVersion, mPathConfigVersion);
  }
}
