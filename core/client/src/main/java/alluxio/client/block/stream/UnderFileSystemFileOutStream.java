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

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.security.authorization.Mode;

import java.io.FilterOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a streaming API to write to a file in the under file system through an Alluxio
 * worker's data server.
 */
@NotThreadSafe
public final class UnderFileSystemFileOutStream extends FilterOutputStream {
  private static final int TIER_UNUSED = -1;

  /**
   * @param context the file system context
   * @param address the address of an Alluxio worker
   * @param options the options to construct this stream with
   * @return a new {@link UnderFileSystemFileOutStream}
   */
  public static OutputStream create(FileSystemContext context, InetSocketAddress address,
      OutStreamOptions options) {
    return new UnderFileSystemFileOutStream(context, address, options.getUfsPath(),
        options.getOwner(), options.getGroup(), options.getMode());
  }

  private final PacketOutStream mOutStream;

  /**
   * Creates an instance of {@link UnderFileSystemFileOutStream}.
   *
   * @param context the file system context
   * @param address the data server address
   * @param path the ufs file path
   * @param owner the owner of the ufs file
   * @param group the group of the ufs file
   * @param mode the mode of the ufs file
   */
  public UnderFileSystemFileOutStream(FileSystemContext context, InetSocketAddress address,
      String path, String owner, String group, Mode mode) {
    super(PacketOutStream.createNettyPacketOutStream(context, address, Long.MAX_VALUE,
        Protocol.WriteRequest.newBuilder().setSessionId(-1).setTier(TIER_UNUSED)
            .setType(Protocol.RequestType.UFS_FILE).setUfsPath(path).setOwner(owner)
            .setGroup(group).setMode(mode.toShort()).buildPartial()));
    mOutStream = (PacketOutStream) out;
  }

  // Explicitly overriding some write methods which are not efficiently implemented in
  // FilterOutStream.

  @Override
  public void write(byte[] b) {
    mOutStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    mOutStream.write(b, off, len);
  }
}
