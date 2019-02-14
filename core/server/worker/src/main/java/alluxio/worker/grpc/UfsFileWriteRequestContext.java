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

package alluxio.worker.grpc;

import alluxio.resource.CloseableResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;

import java.io.OutputStream;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The UFS File write request internal representation.
 */
@NotThreadSafe
public final class UfsFileWriteRequestContext extends WriteRequestContext<UfsFileWriteRequest> {
  private CloseableResource<UnderFileSystem> mUfsResource;
  private CreateOptions mCreateOptions;
  private OutputStream mOutputStream;

  UfsFileWriteRequestContext(alluxio.grpc.WriteRequest request) {
    super(new UfsFileWriteRequest(request));
  }

  /**
   * @return the create options
   */
  @Nullable
  public CreateOptions getCreateOptions() {
    return mCreateOptions;
  }

  /**
   * @return the output stream
   */
  @Nullable
  public OutputStream getOutputStream() {
    return mOutputStream;
  }

  /**
   * @return the handler of the UFS
   */
  @Nullable
  public CloseableResource<UnderFileSystem> getUfsResource() {
    return mUfsResource;
  }

  /**
   * @param createOptions the create options to set
   */
  public void setCreateOptions(CreateOptions createOptions) {
    mCreateOptions = createOptions;
  }

  /**
   * @param outputStream output stream to set
   */
  public void setOutputStream(OutputStream outputStream) {
    mOutputStream = outputStream;
  }

  /**
   * @param ufsResource UFS to set
   */
  public void setUfsResource(CloseableResource<UnderFileSystem> ufsResource) {
    mUfsResource = ufsResource;
  }
}
