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

import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InternalException;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link Protocol.ReadRequest}s.
 *
 * Protocol: Check {@link alluxio.client.block.stream.NettyDataReader} for additional information.
 * 1. Once a read request is received, the handler creates a {@link PacketReader} which reads
 *    packets from the block worker and pushes them to the buffer.
 * 2. The {@link PacketReader} pauses if there are too many packets in flight, and resumes if there
 *    is room available.
 * 3. The channel is closed if there is any exception during the packet read/write.
 *
 * Threading model:
 * Only two threads are involved at a given point of time: netty I/O thread, packet reader thread.
 * 1. The netty I/O thread accepts the read request, handles write callbacks. If any exception
 *    occurs (e.g. failed to read from netty or write to netty) or the read request is cancelled by
 *    the client, the netty I/O thread notifies the packet reader thread.
 * 2. The packet reader thread keeps reading from the file and writes to netty. Before reading a
 *    new packet, it checks whether there are notifications (e.g. cancel, error), if
 *    there is, handle them properly. See more information about the notifications in the javadoc
 *    of {@link ReadRequestContext}.
 *
 * @param <T> type of read request
 */
@NotThreadSafe
public abstract class AbstractReadHandler<T extends ReadRequestContext<?>>
    extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractReadHandler.class);

  /** The executor to run {@link PacketReader}. */
  private final ExecutorService mPacketReaderExecutor;

  private final ConcurrentHashMap<String, PacketReadTask<T>> mTasksMap =
      new ConcurrentHashMap<>();

  /**
   * Creates an instance of {@link AbstractReadHandler}.
   *
   * @param executorService the executor service to run {@link PacketReader}s
   */
  AbstractReadHandler(ExecutorService executorService) {
    mPacketReaderExecutor = executorService;
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    // Notify all the tasks
    mTasksMap.values().forEach(task -> task.notifyChannelException(
        new Error(new InternalException("Channel has been unregistered"), false)));
    mTasksMap.clear();
    ctx.fireChannelUnregistered();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    if (!acceptMessage(object)) {
      ctx.fireChannelRead(object);
      return;
    }
    Protocol.ReadRequest msg = ((RPCProtoMessage) object).getMessage().asReadRequest();
    if (msg.getCancel()) {
      mTasksMap.values().forEach(PacketReadTask::cancelTask);
      mTasksMap.clear();
      return;
    }

    // Create and submit a task for reading and sending packet
    T requestContext = createRequestContext(msg);
    requestContext.setPosToQueue(requestContext.getRequest().getStart());
    requestContext.setPosToWrite(requestContext.getRequest().getStart());
    PacketReader packetReader = createPacketReader();
    String taskId = UUID.randomUUID().toString();
    PacketReadTask<T> packetReadTask =
        new PacketReadTask<>(taskId, requestContext, ctx.channel(), packetReader);
    mTasksMap.put(taskId, packetReadTask);
    mPacketReaderExecutor.submit(() -> {
      try {
        packetReadTask.call();
      } finally {
        mTasksMap.remove(taskId);
      }
    });
    LOG.debug("taskMap.size(): " + mTasksMap.size());
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception caught in AbstractReadHandler for channel {}:", ctx.channel(), cause);
    // Notify the task that there is an error
    mTasksMap.values().forEach(task -> task.notifyChannelException(
        new Error(AlluxioStatusException.fromThrowable(cause), true)));
    mTasksMap.clear();
  }

  /**
   * Checks whether this object should be processed by this handler.
   *
   * @param object the object
   * @return true if this object should be processed
   */
  protected boolean acceptMessage(Object object) {
    if (!(object instanceof RPCProtoMessage)) {
      return false;
    }
    RPCProtoMessage message = (RPCProtoMessage) object;
    return message.getType() == RPCMessage.Type.RPC_READ_REQUEST;
  }

  /**
   * @param request the block read request
   * @return an instance of read request based on the request read from channel
   */
  protected abstract T createRequestContext(Protocol.ReadRequest request);

  /**
   * Creates a read reader.
   * @return the packet reader for this handler
   */
  protected abstract PacketReader createPacketReader();

  /**
   * A runnable that reads packets and writes them to the channel.
   */
  protected abstract class PacketReader {

    /**
     * Completes the read request. When the request is closed, we should clean up any temporary
     * state it may have accumulated.
     *
     * @param context context of the request to complete
     */
    protected abstract void completeRequest(T context) throws Exception;

    /**
     * Returns the appropriate {@link DataBuffer} representing the data to send, depending on the
     * configurable transfer type.
     *
     * @param context context of the request to complete
     * @param channel the netty channel
     * @param len The length, in bytes, of the data to read from the block
     * @return a {@link DataBuffer} representing the data
     */
    protected abstract DataBuffer getDataBuffer(T context, Channel channel, long offset, int len)
        throws Exception;
  }
}
