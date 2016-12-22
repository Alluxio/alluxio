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

import alluxio.Seekable;
import alluxio.client.PositionedReadable;
import alluxio.client.file.FileSystemContext;
import alluxio.proto.dataserver.Protocol;

import java.io.FilterInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a streaming API to read a file in the under file system through an Alluxio worker's data
 * server.
 */
@NotThreadSafe
public final class UnderFileSystemFileInStream extends FilterInputStream
    implements PositionedReadable, Seekable {
  private final PacketInStream mInStream;

  /**
   * Creates an instance of {@link UnderFileSystemFileInStream}.
   *
   * @param context the file system context
   * @param address the data server address
   * @param ufsFileId the ufs file ID
   * @param length the length of file to read which can be set to LONG.MAX_VALUE if unknown
   */
  public UnderFileSystemFileInStream(FileSystemContext context, InetSocketAddress address,
      long ufsFileId, long length) {
    super(PacketInStream.createNettyPacketInStream(context, address, ufsFileId, -1, -1, length,
        Protocol.RequestType.UFS_FILE));
    mInStream = (PacketInStream) in;
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    return mInStream.positionedRead(pos, b, off, len);
  }

  @Override
  public void seek(long pos) throws IOException {
    mInStream.seek(pos);
  }
}
