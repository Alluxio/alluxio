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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a streaming API to write to a file in the under file system through an Alluxio
 * worker's data server.
 */
@NotThreadSafe
public class UnderFileSystemFileOutStream extends BlockOutStream {
  private static final int TIER_UNUSED = -1;

  /**
   * Creates an instance of {@link UnderFileSystemFileOutStream} that writes to a UFS file.
   *
   * @param context the file system context
   * @param address the data server address
   * @param options the out stream options
   * @return the under file system output stream
   */
  public static UnderFileSystemFileOutStream create(FileSystemContext context,
      WorkerNetAddress address, OutStreamOptions options) throws IOException {
    Protocol.CreateUfsFileOptions ufsFileOptions =
        Protocol.CreateUfsFileOptions.newBuilder().setUfsPath(options.getUfsPath())
            .setOwner(options.getOwner()).setGroup(options.getGroup())
            .setMode(options.getMode().toShort()).setMountId(options.getMountId()).build();

    long packetSize =
        Configuration.getBytes(PropertyKey.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES);
    return new UnderFileSystemFileOutStream(new NettyPacketWriter(context, address, Long.MAX_VALUE,
        Protocol.WriteRequest.newBuilder().setTier(TIER_UNUSED)
            .setType(Protocol.RequestType.UFS_FILE).setCreateUfsFileOptions(ufsFileOptions)
            .buildPartial(), packetSize));
  }

  /**
   * Constructs a new {@link BlockOutStream} with only one {@link PacketWriter}.
   *
   * @param packetWriter the packet writer
   */
  protected UnderFileSystemFileOutStream(PacketWriter packetWriter) {
    super(packetWriter, Long.MAX_VALUE);
  }
}
