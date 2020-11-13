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

import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.WriteResponse;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles block write request. Check more information in
 * {@link AbstractWriteHandler}.
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
    justification = "false positive with superclass generics, "
        + "see more description in https://sourceforge.net/p/findbugs/bugs/1242/")
@NotThreadSafe
public final class BlockWriteHandler extends AbstractWriteHandler<BlockWriteRequestContext> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWriteHandler.class);
  private static final long FILE_BUFFER_SIZE = ServerConfiguration.getBytes(
      PropertyKey.WORKER_FILE_BUFFER_SIZE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  private final boolean mDomainSocketEnabled;

  /**
   * Creates an instance of {@link BlockWriteHandler}.
   *
   * @param blockWorker the block worker
   * @param responseObserver the stream observer for the write response
   * @param userInfo the authenticated user info
   * @param domainSocketEnabled whether reading block over domain socket
   */
  BlockWriteHandler(BlockWorker blockWorker, StreamObserver<WriteResponse> responseObserver,
      AuthenticatedUserInfo userInfo, boolean domainSocketEnabled) {
    super(responseObserver, userInfo);
    mWorker = blockWorker;
    mDomainSocketEnabled = domainSocketEnabled;
  }

  @Override
  protected BlockWriteRequestContext createRequestContext(alluxio.grpc.WriteRequest msg)
      throws Exception {
    long bytesToReserve = FILE_BUFFER_SIZE;
    if (msg.getCommand().hasSpaceToReserve()) {
      bytesToReserve = msg.getCommand().getSpaceToReserve();
    }
    BlockWriteRequestContext context = new BlockWriteRequestContext(msg, bytesToReserve);
    BlockWriteRequest request = context.getRequest();
    mWorker.createBlockRemote(request.getSessionId(), request.getId(),
        mStorageTierAssoc.getAlias(request.getTier()),
        request.getMediumType(), bytesToReserve);
    if (mDomainSocketEnabled) {
      context.setCounter(MetricsSystem.counter(MetricKey.WORKER_BYTES_WRITTEN_DOMAIN.getName()));
      context.setMeter(MetricsSystem.meter(
          MetricKey.WORKER_BYTES_WRITTEN_DOMAIN_THROUGHPUT.getName()));
    } else {
      context.setCounter(MetricsSystem.counter(MetricKey.WORKER_BYTES_WRITTEN_ALLUXIO.getName()));
      context.setMeter(MetricsSystem.meter(
          MetricKey.WORKER_BYTES_WRITTEN_ALLUXIO_THROUGHPUT.getName()));
    }
    return context;
  }

  @Override
  protected void completeRequest(BlockWriteRequestContext context) throws Exception {
    WriteRequest request = context.getRequest();
    if (context.getBlockWriter() != null) {
      context.getBlockWriter().close();
    }
    mWorker.commitBlock(request.getSessionId(), request.getId(), request.getPinOnCreate());
  }

  @Override
  protected void cancelRequest(BlockWriteRequestContext context) throws Exception {
    WriteRequest request = context.getRequest();
    if (context.getBlockWriter() != null) {
      context.getBlockWriter().close();
    }
    mWorker.abortBlock(request.getSessionId(), request.getId());
  }

  @Override
  protected void cleanupRequest(BlockWriteRequestContext context) throws Exception {
    if (context.getBlockWriter() != null) {
      context.getBlockWriter().close();
    }
    mWorker.cleanupSession(context.getRequest().getSessionId());
  }

  @Override
  protected void flushRequest(BlockWriteRequestContext context)
      throws Exception {
    // This is a no-op because block worker does not support flush currently.
  }

  @Override
  protected void writeBuf(BlockWriteRequestContext context,
      StreamObserver<WriteResponse> observer, DataBuffer buf, long pos) throws Exception {
    Preconditions.checkState(context != null);
    WriteRequest request = context.getRequest();
    long bytesReserved = context.getBytesReserved();
    if (bytesReserved < pos) {
      long bytesToReserve = Math.max(FILE_BUFFER_SIZE, pos - bytesReserved);
      // Allocate enough space in the existing temporary block for the write.
      mWorker.requestSpace(request.getSessionId(), request.getId(), bytesToReserve);
      context.setBytesReserved(bytesReserved + bytesToReserve);
    }
    if (context.getBlockWriter() == null) {
      context.setBlockWriter(
          mWorker.getTempBlockWriterRemote(request.getSessionId(), request.getId()));
    }
    Preconditions.checkState(context.getBlockWriter() != null);
    int sz = buf.readableBytes();
    Preconditions.checkState(context.getBlockWriter().append(buf)  == sz);
  }

  @Override
  protected String getLocationInternal(BlockWriteRequestContext context) {
    return String.format("temp-block-session-%d-id-%d", context.getRequest().getSessionId(),
        context.getRequest().getId());
  }
}
