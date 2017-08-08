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

package alluxio.worker.netty;

import alluxio.proto.dataserver.Protocol;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The UFS File write request internal representation.
 */
@ThreadSafe
public final class UfsFileWriteRequest extends WriteRequest {
  private final String mUfsPath;
  private final Protocol.CreateUfsFileOptions mCreateUfsFileOptions;

  UfsFileWriteRequest(Protocol.WriteRequest request) {
    super(request);
    mUfsPath = request.getCreateUfsFileOptions().getUfsPath();
    mCreateUfsFileOptions = request.getCreateUfsFileOptions();
  }

  /**
   * @return the UFS path
   */
  public String getUfsPath() {
    return mUfsPath;
  }

  /**
   * @return the options to create UFS file
   */
  public Protocol.CreateUfsFileOptions getCreateUfsFileOptions() {
    return mCreateUfsFileOptions;
  }
}
