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

package alluxio.worker.grpc;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.ReadResponse;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.util.logging.SamplingLogger;
import alluxio.wire.BlockReadRequest;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.UnderFileSystemBlockReader;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles block read request. Check more information in {@link AbstractReadHandler}.
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
    justification = "false positive with superclass generics, "
        + "see more description in https://sourceforge.net/p/findbugs/bugs/1242/")
@NotThreadSafe
public final class BlockReadHandler extends AbstractReadHandler<BlockReadRequestContext> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockReadHandler.class);
  private static final long UFS_BLOCK_OPEN_TIMEOUT_MS =
      ServerConfiguration.getMs(PropertyKey.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS);
  private static final Logger SLOW_BUFFER_LOG = new SamplingLogger(LOG, Constants.MINUTE_MS);
  private static final long SLOW_BUFFER_MS =
      ServerConfiguration.getMs(PropertyKey.WORKER_REMOTE_IO_SLOW_THRESHOLD);

  /** The Block Worker. */
  private final BlockWorker mWorker;

  private final boolean mDomainSocketEnabled;

  /**
   * The data reader to read from a local block worker.
   */
  @NotThreadSafe
  public final class BlockDataReader extends DataReader {
    /** The Block Worker. */
    private final BlockWorker mWorker;

    BlockDataReader(BlockReadRequestContext context, StreamObserver<ReadResponse> response,
        BlockWorker blockWorker) {
      super(context, response);
      mWorker = blockWorker;
    }

    @Override
    protected void completeRequest(BlockReadRequestContext context) throws Exception {
      try {
        mWorker.cleanBlockReader(context.getRequest().getSessionId(),
            context.getRequest().getId(), context.getBlockReader());
      } finally {
        context.setBlockReader(null);
      }
    }

    @Override
    protected DataBuffer getDataBuffer(BlockReadRequestContext context, long offset, int len)
        throws Exception {
      @Nullable
      BlockReader blockReader = null;
      // timings
      long openMs = -1;
      long transferMs = -1;
      long startMs = System.currentTimeMillis();
      try {
        openBlock(context);
        openMs = System.currentTimeMillis() - startMs;
        blockReader = context.getBlockReader();
        Preconditions.checkState(blockReader != null);
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(len, len);
        try {
          long startTransferMs = System.currentTimeMillis();
          while (buf.writableBytes() > 0 && blockReader.transferTo(buf) != -1) {
          }
          transferMs = System.currentTimeMillis() - startTransferMs;
          return new NettyDataBuffer(buf);
        } catch (Throwable e) {
          buf.release();
          throw e;
        }
      } finally {
        long durationMs = System.currentTimeMillis() - startMs;
        if (durationMs >= SLOW_BUFFER_MS) {
          // This buffer took much longer than expected
          String prefix = String
              .format("Getting buffer for remote read took longer than %s ms. ", SLOW_BUFFER_MS)
              + "reader: " + (blockReader == null ? "null" : blockReader.getClass().getName());

          String location = blockReader == null ? "null" : blockReader.getLocation();

          // Do not template the reader class, so the sampling log can distinguish between
          // different reader types
          SLOW_BUFFER_LOG.warn(prefix
                  + " location: {} bytes: {} openMs: {} transferMs: {} durationMs: {}",
              location, len, openMs, transferMs, durationMs);
        }
      }
    }

    /**
     * Opens the block if it is not open.
     *
     * @throws Exception if it fails to open the block
     */
    private void openBlock(BlockReadRequestContext context)
        throws Exception {
      if (context.getBlockReader() != null) {
        return;
      }
      BlockReadRequest request = context.getRequest();
      BlockReader reader = mWorker.getBlockReader(request);
      context.setBlockReader(reader);
      if (reader instanceof UnderFileSystemBlockReader) {
        AlluxioURI ufsMountPointUri =
            ((UnderFileSystemBlockReader) reader).getUfsMountPointUri();
        String ufsString = MetricsSystem.escape(ufsMountPointUri);
        context.setBlockReader(reader);

        MetricKey counterKey = MetricKey.WORKER_BYTES_READ_UFS;
        MetricKey meterKey = MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT;
        context.setCounter(MetricsSystem.counterWithTags(counterKey.getName(),
            counterKey.isClusterAggregated(), MetricInfo.TAG_UFS, ufsString));
        context.setMeter(MetricsSystem.meterWithTags(meterKey.getName(),
            meterKey.isClusterAggregated(), MetricInfo.TAG_UFS, ufsString));
      }
    }
  }

  /**
   * Creates an instance of {@link AbstractReadHandler}.
   *
   * @param executorService the executor service to run {@link DataReader}s
   * @param blockWorker the block worker
   * @param responseObserver the response observer of the gRPC stream
   * @param userInfo the authenticated user info
   * @param domainSocketEnabled whether reading block over domain socket
   */
  public BlockReadHandler(ExecutorService executorService, BlockWorker blockWorker,
      StreamObserver<ReadResponse> responseObserver, AuthenticatedUserInfo userInfo,
      boolean domainSocketEnabled) {
    super(executorService, responseObserver, userInfo);
    mWorker = blockWorker;
    mDomainSocketEnabled = domainSocketEnabled;
  }

  @Override
  protected BlockReadRequestContext createRequestContext(alluxio.grpc.ReadRequest request) {
    BlockReadRequestContext context = new BlockReadRequestContext(request);
    if (mDomainSocketEnabled) {
      context.setCounter(MetricsSystem.counter(MetricKey.WORKER_BYTES_READ_DOMAIN.getName()));
      context.setMeter(MetricsSystem
          .meter(MetricKey.WORKER_BYTES_READ_DOMAIN_THROUGHPUT.getName()));
    } else {
      context.setCounter(MetricsSystem.counter(MetricKey.WORKER_BYTES_READ_REMOTE.getName()));
      context.setMeter(MetricsSystem
          .meter(MetricKey.WORKER_BYTES_READ_REMOTE_THROUGHPUT.getName()));
    }
    return context;
  }

  @Override
  protected DataReader createDataReader(BlockReadRequestContext context,
      StreamObserver<ReadResponse> response) {
    return new BlockDataReader(context, response, mWorker);
  }
}
