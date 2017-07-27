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

import io.netty.channel.Channel;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The UFS File write request internal representation.
 */
@NotThreadSafe
final class UfsFileWriteRequest extends AbstractWriteRequest {
  private UfsFileWriteHandler mUfsFileWriteHandler;
  private UnderFileSystem mUnderFileSystem;
  private OutputStream mOutputStream;
  private final String mUfsPath;
  private final Protocol.CreateUfsFileOptions mCreateUfsFileOptions;

  UfsFileWriteRequest(UfsFileWriteHandler ufsFileWriteHandler, Protocol.WriteRequest request)
      throws Exception {
    super(request.getId());
    mUfsFileWriteHandler = ufsFileWriteHandler;
    mUfsPath = request.getCreateUfsFileOptions().getUfsPath();
    mCreateUfsFileOptions = request.getCreateUfsFileOptions();
  }

  @Override
  public void close(Channel channel) throws IOException {
    if (mOutputStream == null) {
      mUfsFileWriteHandler.createUfsFile(channel);
    }
    if (mOutputStream != null) {
      mOutputStream.close();
      mOutputStream = null;
    }
  }

  @Override
  void cancel() throws IOException {
    // TODO(calvin): Consider adding cancel to the ufs stream api.
    if (mOutputStream != null && mUnderFileSystem != null) {
      mOutputStream.close();
      mUnderFileSystem.deleteFile(mUfsPath);
      mOutputStream = null;
    }
  }

  @Override
  void cleanup() throws IOException {
    cancel();
  }

  /**
   * @return the handler of the UFS
   */
  public UnderFileSystem getUnderFileSystem() {
    return mUnderFileSystem;
  }

  /**
   * @return the output stream
   */
  public OutputStream getOutputStream() {
    return mOutputStream;
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

  /**
   * Sets the output stream.
   *
   * @param outputStream output stream to set
   */
  public void setOutputStream(OutputStream outputStream) {
    mOutputStream = outputStream;
  }

  /**
   * Sets the handler of the UFS.
   *
   * @param underFileSystem UFS to set
   */
  public void setUnderFileSystem(UnderFileSystem underFileSystem) {
    mUnderFileSystem = underFileSystem;
  }
}
