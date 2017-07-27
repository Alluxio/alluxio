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

package alluxio.worker.netty;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.nio.channels.GatheringByteChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles block write request. Check more information in
 * {@link AbstractWriteHandler}.
 */
@NotThreadSafe
public final class BlockWriteHandler extends AbstractWriteHandler {
  private static final long FILE_BUFFER_SIZE = Configuration.getBytes(
      PropertyKey.WORKER_FILE_BUFFER_SIZE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  /**
   * Creates an instance of {@link BlockWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   * @param blockWorker the block worker
   */
  BlockWriteHandler(ExecutorService executorService, BlockWorker blockWorker) {
    super(executorService);
    mWorker = blockWorker;
  }

  @Override
  protected boolean acceptMessage(Object object) {
    if (!super.acceptMessage(object)) {
      return false;
    }
    Protocol.WriteRequest request = ((RPCProtoMessage) object).getMessage().asWriteRequest();
    return request.getType() == Protocol.RequestType.ALLUXIO_BLOCK;
  }

  @Override
  protected AbstractWriteRequest createWriteRequest(RPCProtoMessage msg) throws Exception {
    Protocol.WriteRequest request = (msg.getMessage()).asWriteRequest();
    return new BlockWriteRequest(this, request, FILE_BUFFER_SIZE, mWorker);
  }

  @Override
  protected void writeBuf(Channel channel, ByteBuf buf, long pos) throws Exception {
    BlockWriteRequest request = (BlockWriteRequest) getRequest();
    Preconditions.checkState(request != null);
    long bytesReserved = request.getBytesReserved();
    if (bytesReserved < pos) {
      long bytesToReserve = Math.max(FILE_BUFFER_SIZE, pos - bytesReserved);
      // Allocate enough space in the existing temporary block for the write.
      mWorker.requestSpace(request.getSessionId(), request.getId(), bytesToReserve);
      request.setBytesReserved(bytesReserved + bytesToReserve);
    }
    if (request.getBlockWriter() == null) {
      request.setBlockWriter(mWorker.getTempBlockWriterRemote(
          request.getSessionId(), request.getId()));
      request.setCounter(MetricsSystem.workerCounter("BytesWrittenAlluxio"));
    }
    Preconditions.checkState(request.getBlockWriter() != null);
    GatheringByteChannel outputChannel = request.getBlockWriter().getChannel();
    int sz = buf.readableBytes();
    Preconditions.checkState(buf.readBytes(outputChannel, sz) == sz);
  }

  /**
   * @return the tier storage mapping
   */
  public StorageTierAssoc getStorageTierAssoc() {
    return mStorageTierAssoc;
  }
}
