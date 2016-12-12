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

import alluxio.proto.dataserver.Protocol;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

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
   * @param address the data server address
   * @param ufsFileId the UFS file ID
   * @throws IOException if it fails to create the object
   */
  public UnderFileSystemFileOutStream(InetSocketAddress address, long ufsFileId)
      throws IOException {
    super(new PacketOutStream(new NettyPacketWriter(address, ufsFileId, Long.MAX_VALUE, -1,
        Protocol.RequestType.UFS_FILE), Long.MAX_VALUE));
  }

  @Override
  public void write(byte[] b) throws IOException {
    out.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
  }
}
