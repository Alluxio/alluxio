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

import java.io.FilterOutputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a streaming API to write to a file in the under file system through an Alluxio
 * worker's data server.
 */
@NotThreadSafe
public final class UnderFileSystemFileOutStream extends FilterOutputStream {
  /**
   * Creates an instance of {@link UnderFileSystemFileOutStream}.
   *
   * @param fileSystemWorkerClient the file system worker client
   * @param ufsFileId the UFS file ID
   * @throws IOException if it fails to create the object
   */
  public UnderFileSystemFileOutStream(FileSystemWorkerClient fileSystemWorkerClient, long ufsFileId)
      throws IOException {
    super(new PacketOutStream(
        new NettyPacketWriter(fileSystemWorkerClient.getWorkerDataServerAddress(), ufsFileId, -1),
        Long.MAX_VALUE));
  }
}
