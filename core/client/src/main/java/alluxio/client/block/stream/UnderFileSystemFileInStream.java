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

import java.io.FilterInputStream;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a streaming API to read a file in the under file system through an Alluxio worker's data
 * server.
 */
@NotThreadSafe
public final class UnderFileSystemFileInStream extends FilterInputStream {
  /**
   * Creates an instance of {@link UnderFileSystemFileInStream}.
   *
   * @param address the data server address
   * @param ufsFileId the ufs file ID
   */
  public UnderFileSystemFileInStream(InetSocketAddress address, long ufsFileId) {
    super(new PacketInStream(new NettyPacketReader.Factory(address, ufsFileId), ufsFileId,
        Long.MAX_VALUE));
  }
}
