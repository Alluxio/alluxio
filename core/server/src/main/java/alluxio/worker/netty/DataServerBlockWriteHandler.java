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

import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.network.protocol.RPCBlockWriteRequest;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link RPCBlockWriteRequest}s.
 *
 * Protocol: Check {@link alluxio.client.block.stream.NettyBlockWriter} for more information.
 * 1. The netty channel handler streams packets from the channel and buffers them. The netty
 *    reader is paused if the buffer is full by turning off the auto read, and is resumed when
 *    the buffer is not full.
 * 2. The {@link PacketWriter} polls packets from the buffer and writes to the block worker. The
 *    writer becomes inactive if there is nothing on the buffer to free up the executor. It is
 *    resumed when the buffer becomes non-empty.
 * 3. When an error occurs, the channel is closed. All the buffered packets are released when the
 *    channel is deregistered.
 */
@NotThreadSafe
public abstract class DataServerBlockWriteHandler extends DataServerWriteHandler {
  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();;

  private class BlockWriteRequestInternal extends WriteRequestInternal {
    public BlockWriter mBlockWriter;

    public BlockWriteRequestInternal(RPCBlockWriteRequest request) throws Exception {
      mBlockWriter = mWorker.getTempBlockWriterRemote(request.getSessionId(), request.getBlockId());
      mSessionId = request.getSessionId();
      mId = request.getBlockId();
    }

    @Override
    public void close() throws IOException {
      mBlockWriter.close();
    }
  }

  /**
   * Creates an instance of {@link DataServerBlockWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s.
   */
  public DataServerBlockWriteHandler(ExecutorService executorService, BlockWorker blockWorker) {
    super(executorService);
    mWorker = blockWorker;
  }

  /**
   * Initializes the handler if necessary.
   *
   * @param msg the block write request
   * @throws Exception if it fails to initialize
   */
  protected void initializeRequest(RPCBlockWriteRequest msg) throws Exception {
    super.initializeRequest(msg);
    if (mRequest == null) {
      mRequest = new BlockWriteRequestInternal(msg);
    }
  }

  protected void writeBuf(ByteBuf buf) throws Exception {
    try {
      if (mPosToWrite == 0) {
        // This is the first write to the block, so create the temp block file. The file will only
        // be created if the first write starts at offset 0. This allocates enough space for the
        // write.
        mWorker.createBlockRemote(mRequest.mSessionId, mRequest.mId, mStorageTierAssoc.getAlias(0),
            buf.readableBytes());
      } else {
        // Allocate enough space in the existing temporary block for the write.
        mWorker.requestSpace(mRequest.mSessionId, mRequest.mId, buf.readableBytes());
      }
      BlockWriter blockWriter = ((BlockWriteRequestInternal) mRequest).mBlockWriter;
      GatheringByteChannel outputChannel = blockWriter.getChannel();
      buf.readBytes(outputChannel, buf.readableBytes());
    } finally {
      ReferenceCountUtil.release(buf);
    }
  }
}
