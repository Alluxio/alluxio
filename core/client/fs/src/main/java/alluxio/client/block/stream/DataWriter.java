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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.Cancelable;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.grpc.RequestType;
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
 * The interface to write data.
 */
public interface DataWriter extends Closeable, Cancelable {

  /**
   * Factory for {@link DataWriter}.
   */
  @ThreadSafe
  class Factory {
    public static final Logger LOG = LoggerFactory.getLogger(DataWriter.Factory.class);

    private Factory() {} // prevent instantiation

    /**
     * @param context the file system context
     * @param blockId the block ID
     * @param blockSize the block size in bytes
     * @param address the Alluxio worker address
     * @param options the out stream options
     * @return the {@link DataWriter} instance
     */
    public static DataWriter create(FileSystemContext context, long blockId, long blockSize,
        WorkerNetAddress address, OutStreamOptions options) throws IOException {
      AlluxioConfiguration alluxioConf = context.getClusterConf();
      boolean shortCircuit = alluxioConf.getBoolean(PropertyKey.USER_SHORT_CIRCUIT_ENABLED);
      boolean shortCircuitPreferred =
          alluxioConf.getBoolean(PropertyKey.USER_SHORT_CIRCUIT_PREFERRED);
      boolean ufsFallbackEnabled = options.getWriteType() == WriteType.ASYNC_THROUGH
          && alluxioConf.getBoolean(PropertyKey.USER_FILE_UFS_TIER_ENABLED);
      boolean workerIsLocal = CommonUtils.isLocalHost(address, alluxioConf);

      if (workerIsLocal && context.hasProcessLocalWorker() && !ufsFallbackEnabled) {
        LOG.debug("Creating worker process local output stream for block {} @ {}",
            blockId, address);
        return BlockWorkerDataWriter.create(context, blockId, blockSize, options);
      }
      LOG.debug("Doesn't create worker process local output stream for block {} @ {} "
          + "(data locates in local worker: {}, client locates in local worker process: {}, "
          + "ufs fallback enabled: {})", blockId, address,
          workerIsLocal, context.hasProcessLocalWorker(), ufsFallbackEnabled);

      boolean domainSocketSupported = NettyUtils.isDomainSocketSupported(address);
      if (workerIsLocal && shortCircuit
          && (shortCircuitPreferred || !domainSocketSupported)) {
        if (ufsFallbackEnabled) {
          LOG.info("Creating UFS-fallback short circuit output stream for block {} @ {}", blockId,
              address);
          return UfsFallbackLocalFileDataWriter.create(
              context, address, blockId, blockSize, options);
        }
        LOG.debug("Creating short circuit output stream for block {} @ {}", blockId, address);
        return LocalFileDataWriter.create(context, address, blockId, blockSize, options);
      } else {
        LOG.debug("Creating gRPC output stream for block {} @ {} from client {} "
            + "(data locates in local worker: {}, shortCircuitEnabled: {}, "
            + "shortCircuitPreferred: {}, domainSocketSupported: {})",
            blockId, address, NetworkAddressUtils.getClientHostName(alluxioConf),
            workerIsLocal, shortCircuit, shortCircuitPreferred, domainSocketSupported);
        return GrpcDataWriter
            .create(context, address, blockId, blockSize, RequestType.ALLUXIO_BLOCK,
                options);
      }
    }
  }

  /**
   * Writes a chunk. This method takes the ownership of this chunk even if it fails to write
   * the chunk.
   *
   * @param chunk the chunk
   */
  void writeChunk(ByteBuf chunk) throws IOException;

  /**
   *  Flushes all the pending chunks.
   */
  void flush() throws IOException;

  /**
   * @return the chunk size in bytes used
   */
  int chunkSize();

  /**
   * @return the current pos which is the same as the totally number of bytes written so far
   */
  long pos();
}
