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

import alluxio.client.file.dora.netty.DebugLoggingTracer;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.util.CommonUtils;

import com.codahale.metrics.Counter;
import com.github.oxo42.stateless4j.StateMachine;
import com.github.oxo42.stateless4j.StateMachineConfig;
import com.github.oxo42.stateless4j.transitions.Transition;
import com.github.oxo42.stateless4j.triggers.TriggerWithParameters1;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 *  State machine of Netty Server in Alluxio Worker.
 *  <br>
 *  You can get a diagram describing the state transitions of this state machine, by calling
 *  {@link #generateStateDiagram(Path)}. After you update the states and triggers, create a diagram
 *  and check if the new state transitions are properly handled.
 *
 * @param <T> the ReadRequestContext type
 */
public class PacketReadTaskStateMachine<T extends ReadRequestContext<?>> {
  private static final Logger LOG = LoggerFactory.getLogger(PacketReadTaskStateMachine.class);
  private final StateMachine<State, TriggerEvent> mStateMachine;

  private final AtomicReference<Runnable> mNextTriggerEvent = new AtomicReference<>();

  private final AbstractReadHandler<T>.PacketReader mPacketReader;

  private static final long MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS);

  private final BlockingQueue<SendDataEvent> mFlowControlQueue =
      new ArrayBlockingQueue<>((int) MAX_PACKETS_IN_FLIGHT);

  private final TriggerEventsWithParam mTriggerEventsWithParam;

  private final Channel mChannel;

  private final ReadRequest mRequest;

  /**
   * This is only created in the netty I/O thread when a read request is received, reset when
   * another request is received.
   * Using "volatile" because we want any value change of this variable to be
   * visible across both netty and I/O threads, meanwhile no atomicity of operation is assumed;
   */
  private final T mContext;

  enum TriggerEvent {
    START,
    EOF,
    CANCELLED,
    SEND_DATA_AFTER_READING,
    OUTPUT_LENGTH_NOT_FULFILLED,
    OUTPUT_LENGTH_FULFILLED,
    CHANNEL_EXCEPTION,
    READ_DATA_ERROR,
    SEND_DATA_ERROR,
    COMPLETE_REQUEST_ERROR,
    END
  }

  enum State {
    CREATED,
    READING_DATA,
    SENDING_DATA,
    CHANNEL_ACTIVE,
    TERMINATED,
    TERMINATED_NORMALLY,
    TERMINATED_EXCEPTIONALLY,
    COMPLETED
  }

  /**
   * Trigger events with parameters.
   */
  public static class TriggerEventsWithParam {

    public final TriggerWithParameters1<Error, TriggerEvent> mFailToReadEvent;
    public final TriggerWithParameters1<Error, TriggerEvent> mChannelExceptionEvent;
    public final TriggerWithParameters1<SendDataEvent, TriggerEvent> mSendDataEvent;
    public final TriggerWithParameters1<Error, TriggerEvent> mFailToSendDataEvent;
    public final TriggerWithParameters1<Error, TriggerEvent> mFailToCompleteRequestEvent;

    /**
     * Trigger event with parameters for @{NettyClientStateMacine}.
     *
     * @param config the {@code StateMachineConfig} for binding trigger event with parameter
     *               for {@code StateMachine}
     */
    public TriggerEventsWithParam(StateMachineConfig<State, TriggerEvent> config) {
      mFailToReadEvent =
          config.setTriggerParameters(TriggerEvent.READ_DATA_ERROR, Error.class);
      mChannelExceptionEvent =
          config.setTriggerParameters(TriggerEvent.CHANNEL_EXCEPTION, Error.class);
      mSendDataEvent =
          config.setTriggerParameters(TriggerEvent.SEND_DATA_AFTER_READING, SendDataEvent.class);
      mFailToSendDataEvent =
          config.setTriggerParameters(TriggerEvent.SEND_DATA_ERROR, Error.class);
      mFailToCompleteRequestEvent =
          config.setTriggerParameters(TriggerEvent.COMPLETE_REQUEST_ERROR, Error.class);
    }
  }

  /**
   * The state machine inside {@link PacketReadTask} is for reading packet task in Alluxio Worker.
   * @param context requestContext
   * @param channel the netty channel
   * @param packetReader the packet reader for reading packets
   */
  public PacketReadTaskStateMachine(
      T context, Channel channel, AbstractReadHandler<T>.PacketReader packetReader) {
    StateMachineConfig<State, TriggerEvent> config = new StateMachineConfig<>();
    mTriggerEventsWithParam = new TriggerEventsWithParam(config);
    config.configure(State.CREATED)
        .permit(TriggerEvent.START, State.READING_DATA);
    config.configure(State.READING_DATA)
        .onEntry(this::onReadingData)
        .permit(TriggerEvent.SEND_DATA_AFTER_READING, State.SENDING_DATA)
        .permit(TriggerEvent.READ_DATA_ERROR, State.TERMINATED_EXCEPTIONALLY)
        .permit(TriggerEvent.SEND_DATA_ERROR, State.TERMINATED_EXCEPTIONALLY)
        .permit(TriggerEvent.CHANNEL_EXCEPTION, State.TERMINATED_EXCEPTIONALLY)
        .permit(TriggerEvent.CANCELLED, State.TERMINATED_NORMALLY);
    config.configure(State.SENDING_DATA)
        .onEntryFrom(mTriggerEventsWithParam.mSendDataEvent, this::onSendingData)
        .permit(TriggerEvent.OUTPUT_LENGTH_NOT_FULFILLED, State.READING_DATA)
        .permit(TriggerEvent.OUTPUT_LENGTH_FULFILLED, State.TERMINATED_NORMALLY)
        .permit(TriggerEvent.SEND_DATA_ERROR, State.TERMINATED_EXCEPTIONALLY)
        .permit(TriggerEvent.CHANNEL_EXCEPTION, State.TERMINATED_EXCEPTIONALLY)
        .permit(TriggerEvent.CANCELLED, State.TERMINATED_NORMALLY);
    config.configure(State.TERMINATED_EXCEPTIONALLY)
        .onEntryFrom(mTriggerEventsWithParam.mFailToReadEvent, this::onTerminatedExceptionally)
        .onEntryFrom(mTriggerEventsWithParam.mFailToSendDataEvent, this::onTerminatedExceptionally)
        .onEntryFrom(mTriggerEventsWithParam.mFailToCompleteRequestEvent,
            this::onTerminatedExceptionally)
        .permit(TriggerEvent.END, State.COMPLETED);
    config.configure(State.TERMINATED_NORMALLY)
        .onEntryFrom(TriggerEvent.OUTPUT_LENGTH_FULFILLED, this::onEof)
        .onEntryFrom(TriggerEvent.CANCELLED, this::onCancelled)
        .onEntry(this::onTerminatedNormally)
        .permit(TriggerEvent.END, State.COMPLETED)
        .permit(TriggerEvent.COMPLETE_REQUEST_ERROR, State.TERMINATED_EXCEPTIONALLY);
    config.configure(State.COMPLETED)
        .onEntry(this::onCompleted);

    mStateMachine = new StateMachine<>(State.CREATED, config);
    mStateMachine.setTrace(new DebugLoggingTracer<>(LOG));
    mStateMachine.fireInitialTransition();
    mContext = context;
    mRequest = context.getRequest();
    mChannel = channel;
    mPacketReader = packetReader;
  }

  /**
   * Generates a diagram describing the state transition in a .dot file.
   * Only used for testing purposes.
   *
   * @param outputFile path to the output file
   * @throws IOException when writing file fails
   */
  @VisibleForTesting
  public void generateStateDiagram(Path outputFile) throws IOException {
    try (OutputStream outFile = Files.newOutputStream(outputFile,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      mStateMachine.configuration().generateDotFileInto(outFile, /* printLabels */ true);
    }
  }

  /**
   * Start the state machine.
   */
  public void run() {
    Preconditions.checkState(mStateMachine.isInState(State.CREATED),
        "state machine cannot be restarted: expected initial state %s, "
            + "encountered %s", State.CREATED, mStateMachine.getState());
    fireNext(TriggerEvent.START);
    try {
      for (Runnable trigger = mNextTriggerEvent.getAndSet(null);
           trigger != null;
           trigger = mNextTriggerEvent.getAndSet(null)) {
        trigger.run();
      }
    } catch (RuntimeException e) {
      LOG.error("Unexpected exception during execution, state: {}", mStateMachine.getState(), e);
      // close and release the channel in case there is a programming error in the state machine
      if (mChannel != null) {
        CommonUtils.closeChannel(mChannel);
      }
      throw e;
    }

    Preconditions.checkState(mStateMachine.isInState(State.COMPLETED),
        "execution of state machine has stopped but it is not in a terminated state");
  }

  private void onReadingData() {
    long start = mContext.getPosToQueue();
    int packetSize = (int) Math
        .min(mRequest.getEnd() - mContext.getPosToQueue(), mRequest.getPacketSize());

    // packetSize should always be >= 0 here when reaches here.
    Preconditions.checkState(packetSize >= 0);

    DataBuffer packet = null;
    try {
      packet = getDataBuffer(mContext, mChannel, start, packetSize);
    } catch (Exception e) {
      LOG.error("Failed to read data.", e);
      fireNext(mTriggerEventsWithParam.mFailToReadEvent,
          new Error(AlluxioStatusException.fromThrowable(e), true));
    }

    // Put an object into the flow control queue. If the queue is full, it indicates that there are
    // too many  in-flight packets, and we need to wait some time.
    LOG.debug("Putting an object into the flow control queue.");
    SendDataEvent sendDataEvent = new SendDataEvent(start, packetSize, packet);
    if (packet != null) {
      mFlowControlQueue.offer(sendDataEvent);
      LOG.debug("An object has been put into the flow control queue successfully.");
    }
    fireNext(mTriggerEventsWithParam.mSendDataEvent, sendDataEvent);
  }

  private void onSendingData(SendDataEvent event, Transition<State, TriggerEvent> transition) {
    long start = event.getStart();
    long packetSize = event.getPacketSize();
    DataBuffer packet = event.getPacket();
    if (packet != null) {
      mContext.setPosToQueue(mContext.getPosToQueue() + packet.getLength());
      // Send data to client
      RPCProtoMessage response = RPCProtoMessage.createOkResponse(packet);
      mChannel.writeAndFlush(response).addListener(
          new WriteListener(packet, start + packetSize));
    }

    if (packet == null || packet.getLength() < packetSize || start + packetSize == mRequest
        .getEnd()) {
      // This can happen if the requested read length is greater than the actual length of the
      // block or file starting from the given offset.
      fireNext(TriggerEvent.OUTPUT_LENGTH_FULFILLED);
      return;
    }

    // Continue to read data as it has not been finished yet
    fireNext(TriggerEvent.OUTPUT_LENGTH_NOT_FULFILLED);
  }

  private void onEof() {
    replyEof();
  }

  private void onTerminatedNormally() {
    try {
      while (!mFlowControlQueue.isEmpty()) {
        // wait for finishing sending data
        LOG.debug("mFlowControlQueue.size(): " + mFlowControlQueue.size());
      }
      completeRequest(mContext);
      fireNext(TriggerEvent.END);
    } catch (IOException e) {
      fireNext(mTriggerEventsWithParam.mFailToCompleteRequestEvent,
          new Error(AlluxioStatusException.fromIOException(e), true));
    } catch (Exception e) {
      fireNext(mTriggerEventsWithParam.mFailToCompleteRequestEvent,
          new Error(AlluxioStatusException.fromThrowable(e), true));
    }
  }

  private void onCompleted() {
    // do nothing
  }

  private void onCancelled() {
    replyCancel();
  }

  private void onTerminatedExceptionally(Error error, Transition<State, TriggerEvent> transition) {
    Preconditions.checkNotNull(error, "error");
    mContext.setError(error);

    while (!mFlowControlQueue.isEmpty()) {
      try {
        SendDataEvent sendDataEvent = mFlowControlQueue.poll(200, TimeUnit.MILLISECONDS);
        if (sendDataEvent != null) {
          DataBuffer packet = sendDataEvent.getPacket();
          if (packet != null) {
            packet.release();
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to release packet.", e);
      }
    }

    try {
      completeRequest(mContext);
    } catch (Exception e) {
      LOG.error("Failed to close the request.", e);
    }
    if (error.isNotifyClient()) {
      replyError(error.getCause());
    }
    fireNext(TriggerEvent.END);
  }

  /**
   * Completes the read request. When the request is closed, we should clean up any temporary
   * state it may have accumulated.
   *
   * @param context context of the request to complete
   */
  protected void completeRequest(T context) throws Exception {
    mPacketReader.completeRequest(context);
  }

  /**
   * Cancel this packet reading task.
   */
  public void cancel() {
    fireNext(TriggerEvent.CANCELLED);
  }

  /**
   * Notify the state machine that a channel exception occur.
   * @param e the encountered Error object
   */
  public void occurChannelException(Error e) {
    fireNext(mTriggerEventsWithParam.mChannelExceptionEvent, e);
  }

  /**
   * Writes an error read response to the channel and closes the channel after that.
   */
  private void replyError(AlluxioStatusException e) {
    mChannel.writeAndFlush(RPCProtoMessage.createResponse(e))
        .addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Writes a success response.
   */
  private void replyEof() {
    Preconditions.checkState(!mContext.isDoneUnsafe());
    mContext.setDoneUnsafe(true);
    mChannel.writeAndFlush(RPCProtoMessage.createOkResponse(null))
        .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  /**
   * Writes a cancel response.
   */
  private void replyCancel() {
    Preconditions.checkState(!mContext.isDoneUnsafe());
    mContext.setDoneUnsafe(true);
    mChannel.writeAndFlush(RPCProtoMessage.createCancelResponse())
        .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  /**
   * Returns the appropriate {@link DataBuffer} representing the data to send, depending on the
   * configurable transfer type.
   *
   * @param context context of the request to complete
   * @param channel the netty channel
   * @param len     The length, in bytes, of the data to read from the block
   * @return a {@link DataBuffer} representing the data
   */
  protected DataBuffer getDataBuffer(T context, Channel channel, long offset, int len)
      throws Exception {
    return mPacketReader.getDataBuffer(context, channel, offset, len);
  }

  /**
   * Helper method to allow firing triggers within state handler methods.
   * If the triggers are fired directly within state handler methods, they will likely make
   * recursive calls and blow up the stack.
   *
   * @param triggerEvent the next trigger event to fire
   */
  private void fireNext(TriggerEvent triggerEvent) {
    mNextTriggerEvent.set(() -> mStateMachine.fire(triggerEvent));
  }

  /**
   * Helper method to allow firing triggers within state handler methods.
   * If the triggers are fired directly within state handler methods, they will likely make
   * recursive calls and blow up the stack.
   *
   * @param triggerEvent the next trigger event to fire
   * @param arg0         the argument to be used
   * @param <Arg0T>      the type of the argument to be used
   */
  private <Arg0T> void fireNext(
      TriggerWithParameters1<Arg0T, TriggerEvent> triggerEvent, Arg0T arg0) {
    mNextTriggerEvent.set(() -> mStateMachine.fire(triggerEvent, arg0));
  }

  private static class SendDataEvent {

    private final long mStart;

    private final long mPacketSize;

    private final DataBuffer mPacket;

    SendDataEvent(long start, long packetSize, DataBuffer packet) {
      mStart = start;
      mPacketSize = packetSize;
      mPacket = packet;
    }

    public long getStart() {
      return mStart;
    }

    public long getPacketSize() {
      return mPacketSize;
    }

    public DataBuffer getPacket() {
      return mPacket;
    }
  }

  /**
   * The channel handler listener that runs after a packet write is flushed.
   */
  private final class WriteListener implements ChannelFutureListener {

    private final DataBuffer mDataBuffer;
    private final long mPosToWriteUncommitted;

    /**
     * Creates an instance of the {@link WriteListener}.
     *
     * @param posToWriteUncommitted the position to commit (i.e. update mPosToWrite)
     */
    WriteListener(DataBuffer dataBuffer, long posToWriteUncommitted) {
      mDataBuffer = dataBuffer;
      mPosToWriteUncommitted = posToWriteUncommitted;
    }

    /**
     * @param bytesRead bytes read
     */
    private void incrementMetrics(long bytesRead) {
      Counter counter = mContext.getCounter();
      Preconditions.checkState(counter != null);
      counter.inc(bytesRead);
    }

    @Override
    public void operationComplete(ChannelFuture future) throws InterruptedException {
      if (!future.isSuccess()) {
        LOG.error("Failed to send packet.", future.cause());
        mFlowControlQueue.take();
        if (mDataBuffer != null) {
          mDataBuffer.release();
        }
        fireNext(mTriggerEventsWithParam.mFailToSendDataEvent,
            new Error(AlluxioStatusException.fromThrowable(future.cause()), true));
        return;
      }

      // Commit the mPosToWrite in mContext as the data have been sent to client successfully
      Preconditions.checkState(
          mPosToWriteUncommitted - mContext.getPosToWrite() <= mContext.getRequest()
              .getPacketSize(), "Some packet is not acked.");
      incrementMetrics(mPosToWriteUncommitted - mContext.getPosToWrite());
      mContext.setPosToWrite(mPosToWriteUncommitted);

      LOG.debug("Taking an object from the flow control queue successfully.");
      mFlowControlQueue.take();
      LOG.debug("An object has been taken from the flow control queue successfully.");
    }
  }
}

