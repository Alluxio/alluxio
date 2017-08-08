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
import alluxio.underfs.UnderFileSystem;

import java.io.OutputStream;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The UFS File write request internal representation.
 */
@NotThreadSafe
public final class UfsFileWriteRequestContext extends WriteRequestContext<UfsFileWriteRequest> {
  private UnderFileSystem mUnderFileSystem;
  private OutputStream mOutputStream;

  UfsFileWriteRequestContext(Protocol.WriteRequest request) {
    super(new UfsFileWriteRequest(request));
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
  public UnderFileSystem getUnderFileSystem() {
    return mUnderFileSystem;
  }

  /**
   * @param outputStream output stream to set
   */
  public void setOutputStream(OutputStream outputStream) {
    mOutputStream = outputStream;
  }

  /**
   * @param underFileSystem UFS to set
   */
  public void setUnderFileSystem(UnderFileSystem underFileSystem) {
    mUnderFileSystem = underFileSystem;
  }
}
