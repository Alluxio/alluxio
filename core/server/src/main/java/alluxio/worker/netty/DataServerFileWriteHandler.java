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

import alluxio.Constants;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.network.protocol.RPCBlockWriteRequest;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.file.FileSystemWorker;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
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
public abstract class DataServerFileWriteHandler extends DataServerWriteHandler {
  /** Filesystem worker which handles file level operations for the worker. */
  private final FileSystemWorker mWorker;

  private class FileWriteRequestInternal extends WriteRequestInternal {
    public OutputStream mOutputStream;

    public FileWriteRequestInternal(RPCBlockWriteRequest request) throws Exception {
      mOutputStream = mWorker.getUfsOutputStream(request.getBlockId());
      mId = request.getBlockId();
    }

    @Override
    public void close() throws IOException {}
  }

  /**
   * Creates an instance of {@link BlockWriteDataServerHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s.
   */
  public DataServerFileWriteHandler(ExecutorService executorService, FileSystemWorker worker) {
    super(executorService);
    mWorker = worker;
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
      mRequest = new FileWriteRequestInternal(msg);
    }
  }

  protected void writeBuf(ByteBuf buf) throws Exception {
    try {
      // This channel will not be closed because the underlying stream should not be closed, the
      // channel will be cleaned up when the underlying stream is closed.
      WritableByteChannel channel = Channels.newChannel(((FileWriteRequestInternal) mRequest).mOutputStream);
      if (buf.nioBufferCount() > 0) {
        ByteBuffer[] buffers = buf.nioBuffers();
        for (int i = 0; i < buffers.length; i++) {
          channel.write(buffers[i]);
        }
      } else {
        ByteBuffer buffer = ByteBuffer.allocateDirect(buf.readableBytes());
        buf.readBytes(buffer);
        channel.write(buffer);
      }
    } finally {
      ReferenceCountUtil.release(buf);
    }
  }
}
