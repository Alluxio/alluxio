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
import alluxio.client.Cancelable;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.grpc.WritePType;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.network.NettyUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The interface to write packets.
 */
public interface PacketWriter extends Closeable, Cancelable {

  /**
   * Factory for {@link PacketWriter}.
   */
  @ThreadSafe
  class Factory {
    public static final Logger LOG = LoggerFactory.getLogger(PacketWriter.Factory.class);

    private Factory() {} // prevent instantiation

    /**
     * @param context the file system context
     * @param blockId the block ID
     * @param blockSize the block size in bytes
     * @param address the Alluxio worker address
     * @param options the out stream options
     * @return the {@link PacketWriter} instance
     */
    public static PacketWriter create(FileSystemContext context, long blockId, long blockSize,
        WorkerNetAddress address, OutStreamOptions options) throws IOException {
      if (CommonUtils.isLocalHost(address) && Configuration
          .getBoolean(PropertyKey.USER_SHORT_CIRCUIT_ENABLED) && !NettyUtils
          .isDomainSocketSupported(address)) {
        if (options.getWriteType() == WritePType.WRITE_ASYNC_THROUGH
            && Configuration.getBoolean(PropertyKey.USER_FILE_UFS_TIER_ENABLED)) {
          LOG.info("Creating UFS-fallback short circuit output stream for block {} @ {}", blockId,
              address);
          return UfsFallbackLocalFilePacketWriter.create(
              context, address, blockId, blockSize, options);
        }
        LOG.debug("Creating short circuit output stream for block {} @ {}", blockId, address);
        return LocalFilePacketWriter.create(context, address, blockId, options);
      } else {
        LOG.debug("Creating netty output stream for block {} @ {} from client {}", blockId, address,
            NetworkAddressUtils.getClientHostName());
        return NettyPacketWriter
            .create(context, address, blockId, blockSize, Protocol.RequestType.ALLUXIO_BLOCK,
                options);
      }
    }
  }

  /**
   * Writes a packet. This method takes the ownership of this packet even if it fails to write
   * the packet.
   *
   * @param packet the packet
   */
  void writePacket(ByteBuf packet) throws IOException;

  /**
   *  Flushes all the pending packets.
   */
  void flush() throws IOException;

  /**
   * @return the packet size in bytes used
   */
  int packetSize();

  /**
   * @return the current pos which is the same as the totally number of bytes written so far
   */
  long pos();
}
