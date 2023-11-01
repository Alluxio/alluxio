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

import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
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
public abstract class AbstractReadHandler<T extends ReadRequest>
    extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractReadHandler.class);

  /** The executor to run {@link NettyReadHandlerStateMachine}. */
  private final ExecutorService mPacketReaderExecutor;
  private final NettyReadHandlerStateMachine<T> mStateMachine;

  /**
   * Creates an instance of {@link AbstractReadHandler}.
   *
   * @param executorService the executor service to run {@link PacketReader}s
   */
  protected AbstractReadHandler(ExecutorService executorService, Channel channel,
      Class<T> requestType,
      PacketReader.Factory<T, ? extends PacketReader<T>> packetReaderFactory) {
    mPacketReaderExecutor = executorService;
    mStateMachine = new NettyReadHandlerStateMachine<>(channel, requestType, packetReaderFactory);
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    mPacketReaderExecutor.submit(mStateMachine::run);
    ctx.fireChannelRegistered();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    mStateMachine.notifyChannelClosed();
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
      mStateMachine.cancel();
      return;
    }

    // Create and submit a task for reading and sending packet
    T request = createReadRequest(msg);
    mStateMachine.submitNewRequest(request);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception caught in AbstractReadHandler for channel {}:", ctx.channel(), cause);
    mStateMachine.notifyChannelException(cause);
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
  protected abstract T createReadRequest(Protocol.ReadRequest request);

  /**
   * A runnable that reads packets and writes them to the channel.
   * @param <T> type of read request
   */
  public interface PacketReader<T extends ReadRequest> extends Closeable {

    /**
     * Factory that creates a PacketReader.
     *
     * @param <T> type of read requestr
     * @param <P> type of the packet reader created by this factory
     */
    interface Factory<T extends ReadRequest, P extends PacketReader<T>> {
      /**
       * Creates a new packet reader.
       *
       * @param readRequest the read request
       * @return packet reader
       * @throws IOException if IOException occurs
       */
      P create(T readRequest) throws IOException;
    }

    /**
     * Returns the appropriate {@link DataBuffer} representing the data to send, depending on the
     * configurable transfer type.
     *
     * @param channel the netty channel
     * @param offset offset
     * @param len The length, in bytes, of the data to read from the block
     * @return a {@link DataBuffer} representing the data
     */
    DataBuffer createDataBuffer(Channel channel, long offset, int len) throws Exception;
  }
}
