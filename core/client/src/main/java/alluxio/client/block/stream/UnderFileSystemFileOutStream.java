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

package alluxio.client.block.stream;

import alluxio.client.file.FileSystemWorkerClient;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class UnderFileSystemFileOutStream extends PacketOutStream {
  private final FileSystemWorkerClient mFileSystemWorkerClient;

  public UnderFileSystemFileOutStream(FileSystemWorkerClient fileSystemWorkerClient, long ufsFileId)
      throws IOException {
    super(ufsFileId, Long.MAX_VALUE);
    mFileSystemWorkerClient = fileSystemWorkerClient;
  }

  @Override
  protected PacketWriter createPacketWriter() throws IOException {
    return new NettyPacketWriter(mFileSystemWorkerClient.getWorkerDataServerAddress(), mId, -1);
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    updateCurrentPacket(true);
    mPacketWriter.cancel();
    // TODO(peis): Move the FileSystemWorker cancel logic here from FileOutStream.
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      updateCurrentPacket(true);
      mPacketWriter.close();
    } catch (Throwable e) {
      throw mCloser.rethrow(e);
    } finally {
      mCloser.close();
      mClosed = true;
    }
  }
}
