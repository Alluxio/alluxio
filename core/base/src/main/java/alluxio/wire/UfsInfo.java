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

import alluxio.AlluxioURI;
import alluxio.grpc.MountPOptions;

/**
 * Class to represent a ufs info.
 */
public final class UfsInfo {
  private AlluxioURI mUri;
  private MountPOptions mMountOptions;

  /**
   * Creates a new instance of {@link UfsInfo}.
   */
  public UfsInfo() {}

  /**
   * @return the Alluxio URI
   */
  public AlluxioURI getUri() {
    return mUri;
  }

  /**
   * @return the mount options
   */
  public MountPOptions getMountOptions() {
    return mMountOptions;
  }

  /**
   * @param uri the Alluxio URI
   * @return the ufs info
   */
  public UfsInfo setUri(AlluxioURI uri) {
    mUri = uri;
    return this;
  }

  /**
   * @param options the mount options
   * @return the ufs info
   */
  public UfsInfo setMountOptions(MountPOptions options) {
    mMountOptions = options;
    return this;
  }
}
