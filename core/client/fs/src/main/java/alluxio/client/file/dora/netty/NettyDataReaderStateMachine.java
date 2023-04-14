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

package alluxio.client.file.dora.netty;

import static alluxio.client.file.dora.netty.PartialReadException.CauseType;

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.dora.netty.event.ResponseEvent;
import alluxio.client.file.dora.netty.event.ResponseEventContext;
import alluxio.client.file.dora.netty.event.ResponseEventFactory;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnavailableException;
import alluxio.file.ReadTargetBuffer;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.network.NettyUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.util.proto.ProtoUtils;
import alluxio.wire.WorkerNetAddress;

import com.github.oxo42.stateless4j.StateMachine;
import com.github.oxo42.stateless4j.StateMachineConfig;
import com.github.oxo42.stateless4j.transitions.Transition;
import com.github.oxo42.stateless4j.triggers.TriggerWithParameters1;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * State machine of Netty Client.
 * <br>
 * You can get a diagram describing the state transitions of this state machine, by calling
 * {@link #generateStateDiagram(Path)}. After you update the states and triggers, create a diagram
 * and check if the new state transitions are properly handled.
 */
@NotThreadSafe
public class NettyDataReaderStateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(NettyDataReaderStateMachine.class);
  private final StateMachine<State, TriggerEvent> mStateMachine;
  private final TriggerEventsWithParam mTriggerEventsWithParam;

  private final AtomicReference<Runnable> mNextTriggerEvent = new AtomicReference<>();

  private final FileSystemContext mContext;
  private final long mReadTimeoutMs;
  private final int mMaxPacketsInFlight;
  private final WorkerNetAddress mAddress;
  private final Supplier<Protocol.ReadRequest.Builder> mRequestBuilder;
  private final int mLength;
  private final ReadTargetBuffer mOutputBuffer;
  private final BlockingQueue<ResponseEvent> mResponseEventQueue = new LinkedBlockingQueue<>();

  @Nullable
  private Channel mChannel;
  private int mBytesRead;
  @Nullable
  private Throwable mLastException;
  @Nullable
  private TriggerEvent mLastExceptionTrigger;

  enum State {
    CREATED,
    TERMINATED,
    TERMINATED_NORMALLY,
    TERMINATED_EXCEPTIONALLY,
    CLIENT_CANCEL,
    CLIENT_CANCEL_DATA_RECEIVED,
    ACQUIRING_CHANNEL,
    CHANNEL_ACTIVE,
    RECEIVED_DATA,
    EXPECTING_EOF,
    RECEIVED_EOF
  }

  /**
   * Trigger event definition for {@code NettyClientStateMachine}.
   */
  public enum TriggerEvent {
    START,
    CHANNEL_AVAILABLE,
    CHANNEL_UNAVAILABLE,
    DATA_AVAILABLE,
    HEART_BEAT,
    TIMEOUT,
    INTERRUPTED,
    EOF,
    OUTPUT_LENGTH_FULFILLED,
    OUTPUT_LENGTH_NOT_FULFILLED,
    OUTPUT_ERROR,
    SERVER_ERROR,
    SERVER_CANCEL,
    CHANNEL_ERROR,
    // this trigger is intentionally left unaccepted by any state
    // so that it will cause an unhandled trigger exception from the state machine
    UNKNOWN_PAYLOAD,
    DATA_DISCARDED,
  }

  /**
   * Trigger event with parameters for @{NettyClientStateMacine}.
   */
  public static class TriggerEventsWithParam {

    public final TriggerWithParameters1<IOException, TriggerEvent> mChannelUnavailableEvent;
    public final TriggerWithParameters1<ByteBuf, TriggerEvent> mDataAvailableEvent;
    public final TriggerWithParameters1<AlluxioStatusException, TriggerEvent> mServerErrorEvent;
    public final TriggerWithParameters1<Throwable, TriggerEvent> mChannelErrorEvent;
    public final TriggerWithParameters1<Throwable, TriggerEvent> mOutputErrorEvent;
    public final TriggerWithParameters1<TimeoutException, TriggerEvent> mTimeoutEvent;
    public final TriggerWithParameters1<InterruptedException, TriggerEvent> mInterruptedEvent;

    /**
     * Trigger event with parameters for @{NettyClientStateMacine}.
     * @param config the {@code StateMachineConfig} for binding trigger event with parameter
     *               for {@code StateMachine}
     */
    public TriggerEventsWithParam(StateMachineConfig<State, TriggerEvent> config) {
      mChannelUnavailableEvent =
          config.setTriggerParameters(TriggerEvent.CHANNEL_UNAVAILABLE, IOException.class);
      mDataAvailableEvent =
          config.setTriggerParameters(TriggerEvent.DATA_AVAILABLE, ByteBuf.class);
      mServerErrorEvent =
          config.setTriggerParameters(TriggerEvent.SERVER_ERROR, AlluxioStatusException.class);
      mChannelErrorEvent =
          config.setTriggerParameters(TriggerEvent.CHANNEL_ERROR, Throwable.class);
      mOutputErrorEvent =
          config.setTriggerParameters(TriggerEvent.OUTPUT_ERROR, Throwable.class);
      mTimeoutEvent =
          config.setTriggerParameters(TriggerEvent.TIMEOUT, TimeoutException.class);
      mInterruptedEvent =
          config.setTriggerParameters(TriggerEvent.INTERRUPTED, InterruptedException.class);
    }
  }

  /**
   * Constructor.
   *
   * @param context
   * @param address
   * @param requestBuilder
   * @param buffer
   */
  public NettyDataReaderStateMachine(
      FileSystemContext context,
      WorkerNetAddress address,
      Protocol.ReadRequest.Builder requestBuilder,
      ReadTargetBuffer buffer) {
    mContext = context;
    AlluxioConfiguration conf = context.getClusterConf();
    mReadTimeoutMs = conf.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
    mMaxPacketsInFlight = conf.getInt(PropertyKey.USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS);
    mAddress = address;
    // clone the builder so that the initial values does not get overridden
    mRequestBuilder = requestBuilder::clone;
    mLength = (int) requestBuilder.getLength();
    mOutputBuffer = buffer;

    StateMachineConfig<State, TriggerEvent> config = new StateMachineConfig<>();
    mTriggerEventsWithParam = new TriggerEventsWithParam(config);

    config.configure(State.CREATED)
        .permit(TriggerEvent.START, State.ACQUIRING_CHANNEL);
    config.configure(State.ACQUIRING_CHANNEL)
        .onEntry(this::acquireNettyChannel)
        .permit(TriggerEvent.CHANNEL_AVAILABLE, State.CHANNEL_ACTIVE, this::sendRequest)
        // no need to cancel
        .permit(TriggerEvent.CHANNEL_UNAVAILABLE, State.TERMINATED_EXCEPTIONALLY);
    config.configure(State.CHANNEL_ACTIVE)
        .onEntry(this::pollResponseFromQueue)
        .permit(TriggerEvent.DATA_AVAILABLE, State.RECEIVED_DATA)
        .permitReentry(TriggerEvent.HEART_BEAT)
        .permit(TriggerEvent.EOF, State.RECEIVED_EOF)
        .permit(TriggerEvent.TIMEOUT, State.CLIENT_CANCEL, this::sendClientCancel)
        .permit(TriggerEvent.INTERRUPTED, State.CLIENT_CANCEL, this::sendClientCancel)
        .permit(TriggerEvent.SERVER_ERROR, State.CLIENT_CANCEL, this::sendClientCancel)
        .permit(TriggerEvent.CHANNEL_ERROR, State.CLIENT_CANCEL, this::sendClientCancel);
    config.configure(State.RECEIVED_DATA)
        .onEntryFrom(mTriggerEventsWithParam.mDataAvailableEvent, this::onReceivedData)
        .permit(TriggerEvent.OUTPUT_ERROR, State.CLIENT_CANCEL, this::sendClientCancel)
        .permit(TriggerEvent.OUTPUT_LENGTH_FULFILLED, State.EXPECTING_EOF)
        .permit(TriggerEvent.OUTPUT_LENGTH_NOT_FULFILLED, State.CHANNEL_ACTIVE);
    config.configure(State.EXPECTING_EOF)
        .onEntry(this::pollResponseFromQueue)
        .permit(TriggerEvent.EOF, State.TERMINATED_NORMALLY)
        // todo(bowen): do we need to handle DATA_AVAILABLE from an insane server?
        // we have got enough data to exit correctly,
        // so just close the channel if anything unexpected happens instead of throwing an error
        .permit(TriggerEvent.TIMEOUT, State.TERMINATED_NORMALLY, this::syncCloseChannel)
        .permit(TriggerEvent.INTERRUPTED, State.TERMINATED_NORMALLY, this::syncCloseChannel)
        .permit(TriggerEvent.SERVER_ERROR, State.TERMINATED_NORMALLY, this::syncCloseChannel)
        .permit(TriggerEvent.CHANNEL_ERROR, State.TERMINATED_NORMALLY, this::syncCloseChannel);
    config.configure(State.RECEIVED_EOF)
        .onEntry(this::onReceivedEof)
        .permit(TriggerEvent.OUTPUT_LENGTH_FULFILLED, State.TERMINATED_NORMALLY)
        .permit(TriggerEvent.OUTPUT_LENGTH_NOT_FULFILLED, State.TERMINATED_NORMALLY);
    config.configure(State.CLIENT_CANCEL)
        .onEntryFrom(mTriggerEventsWithParam.mInterruptedEvent, this::setException)
        .onEntryFrom(mTriggerEventsWithParam.mTimeoutEvent, this::setException)
        .onEntryFrom(mTriggerEventsWithParam.mServerErrorEvent, this::setException)
        .onEntryFrom(mTriggerEventsWithParam.mChannelErrorEvent, this::setException)
        .onEntryFrom(mTriggerEventsWithParam.mOutputErrorEvent, this::setException)
        .onEntry(this::pollResponseFromQueue)
        .permit(TriggerEvent.DATA_AVAILABLE, State.CLIENT_CANCEL_DATA_RECEIVED)
        .permitReentry(TriggerEvent.EOF)
        .permitReentry(TriggerEvent.HEART_BEAT)
        // this is the good case where server has acknowledged client cancel
        // and the channel is OK for reuse
        .permit(TriggerEvent.SERVER_CANCEL, State.TERMINATED_NORMALLY)
        .permit(TriggerEvent.INTERRUPTED, State.TERMINATED_EXCEPTIONALLY)
        .permit(TriggerEvent.TIMEOUT, State.TERMINATED_EXCEPTIONALLY)
        .permit(TriggerEvent.SERVER_ERROR, State.TERMINATED_EXCEPTIONALLY)
        .permit(TriggerEvent.CHANNEL_ERROR, State.TERMINATED_EXCEPTIONALLY);
    config.configure(State.TERMINATED_EXCEPTIONALLY)
        .substateOf(State.TERMINATED)
        .onEntryFrom(mTriggerEventsWithParam.mChannelUnavailableEvent, this::setException)
        .onEntryFrom(mTriggerEventsWithParam.mInterruptedEvent, this::addExceptionAsSuppressed)
        .onEntryFrom(mTriggerEventsWithParam.mTimeoutEvent, this::addExceptionAsSuppressed)
        .onEntryFrom(mTriggerEventsWithParam.mServerErrorEvent, this::addExceptionAsSuppressed)
        .onEntryFrom(mTriggerEventsWithParam.mChannelErrorEvent, this::addExceptionAsSuppressed)
        .onEntry(this::onTerminatedExceptionally);
    config.configure(State.CLIENT_CANCEL_DATA_RECEIVED)
        .onEntryFrom(mTriggerEventsWithParam.mDataAvailableEvent, this::onClientCancelDataReceived)
        .permit(TriggerEvent.DATA_DISCARDED, State.CLIENT_CANCEL);
    config.configure(State.TERMINATED_NORMALLY)
        .substateOf(State.TERMINATED)
        .onEntry(this::onTerminatedNormally);

    mStateMachine = new StateMachine<>(/* initialState */ State.CREATED, config);
    mStateMachine.setTrace(new DebugLoggingTracer<>(LOG));
    mStateMachine.fireInitialTransition();
  }

  /**
   * Helper method to allow firing triggers within state handler methods.
   * If the triggers are fired directly within state handler methods, they will likely make
   * recursive calls and blow up the stack.
   * @param triggerEvent the next trigger event to fire
   */
  public void fireNext(TriggerEvent triggerEvent) {
    mNextTriggerEvent.set(() -> mStateMachine.fire(triggerEvent));
  }

  /**
   * Helper method to allow firing triggers within state handler methods.
   * If the triggers are fired directly within state handler methods, they will likely make
   * recursive calls and blow up the stack.
   * @param triggerEvent the next trigger event to fire
   * @param arg0 the argument to be used
   * @param <Arg0T> the type of the argument to be used
   */
  public <Arg0T> void fireNext(
      TriggerWithParameters1<Arg0T, TriggerEvent> triggerEvent, Arg0T arg0) {
    mNextTriggerEvent.set(() -> mStateMachine.fire(triggerEvent, arg0));
  }

  /**
   * Get the TriggerEventsWithParam.
   * @return the TriggerEventsWithParam object
   */
  public TriggerEventsWithParam getTriggerEventsWithParam() {
    return mTriggerEventsWithParam;
  }

  /**
   * Starts the state machine.
   */
  public void run() {
    Preconditions.checkState(mStateMachine.isInState(State.CREATED),
        "state machine cannot be restarted: expected initial state %s, encountered %s",
        State.CREATED, mStateMachine.getState());
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
        mContext.releaseNettyChannel(mAddress, mChannel);
      }
      throw e;
    }

    Preconditions.checkState(mStateMachine.isInState(State.TERMINATED),
        "execution of state machine has stopped but it is not in a terminated state");
  }

  /**
   * Gets the number of bytes that has been received and written into the output channel.
   *
   * @return number of bytes read
   */
  public int getBytesRead() {
    return mBytesRead;
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
   * Gets the exception during execution.
   *
   * @return exception, or null if there is none
   */
  @Nullable
  public PartialReadException getException() {
    if (mLastExceptionTrigger == null) {
      return null;
    }
    switch (mLastExceptionTrigger) {
      case INTERRUPTED:
        return new PartialReadException(mLength, mBytesRead, CauseType.INTERRUPT, mLastException);
      case OUTPUT_ERROR:
        return new PartialReadException(mLength, mBytesRead, CauseType.OUTPUT, mLastException);
      case TIMEOUT:
        return new PartialReadException(mLength, mBytesRead, CauseType.TIMEOUT, mLastException);
      case SERVER_ERROR:
        return new PartialReadException(
            mLength, mBytesRead, CauseType.SERVER_ERROR, mLastException);
      case CHANNEL_ERROR:
        // fallthrough
      case CHANNEL_UNAVAILABLE:
        return new PartialReadException(
            mLength, mBytesRead, CauseType.TRANSPORT_ERROR, mLastException);
      default:
        throw new IllegalStateException("unexpected trigger type: " + mLastExceptionTrigger);
    }
  }

  void acquireNettyChannel() {
    try {
      mChannel = mContext.acquireNettyChannel(mAddress);
      mChannel.pipeline().addLast(new PacketReadHandler(mResponseEventQueue, mMaxPacketsInFlight));
    } catch (IOException ioe) {
      fireNext(mTriggerEventsWithParam.mChannelUnavailableEvent, ioe);
      return;
    }
    fireNext(TriggerEvent.CHANNEL_AVAILABLE);
  }

  void sendRequest() {
    Preconditions.checkNotNull(mChannel, "channel has not been acquired");
    Protocol.ReadRequest readRequest = mRequestBuilder.get().clearCancel().build();
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(readRequest)))
        .addListener((ChannelFutureListener) future -> {
          if (!future.isSuccess()) {
            // Note: cannot call fireNext(TriggerEvent.CHANNEL_ERROR, future.cause()) directly
            // as the callback is called on a Netty I/O thread, it would bypass the blocking queue
            // and create a race condition with the thread the state machine is executing on
            mResponseEventQueue.offer(ResponseEventFactory.getResponseEventFactory()
                .createChannelErrorResponseEvent(future.cause()));
          }
        });
  }

  void pollResponseFromQueue() {
    if (!tooManyResponseEventsPending(mResponseEventQueue, mMaxPacketsInFlight)) {
      NettyUtils.enableAutoRead(mChannel);
    }
    ResponseEventContext responseEventContext =
        new ResponseEventContext(this);
    ResponseEvent responseEvent;
    try {
      responseEvent = mResponseEventQueue.poll(mReadTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      fireNext(mTriggerEventsWithParam.mInterruptedEvent, interruptedException);
      return;
    }

    if (responseEvent == null) {
      fireNext(mTriggerEventsWithParam.mTimeoutEvent, new TimeoutException(
          "Timed out when waiting for server response for " + mReadTimeoutMs + " ms"));
    } else {
      responseEvent.postProcess(responseEventContext);
    }
  }

  void onReceivedData(ByteBuf buf, Transition<State, TriggerEvent> transition) {
    Preconditions.checkState(TriggerEvent.DATA_AVAILABLE == transition.getTrigger());
    int bytesToWrite = buf.readableBytes();
    try {
      mOutputBuffer.writeBytes(buf);
    } catch (RuntimeException e) {
      fireNext(mTriggerEventsWithParam.mOutputErrorEvent, e);
      return;
    } finally {
      buf.release();
    }
    mBytesRead += bytesToWrite;
    if (mBytesRead < mLength) {
      fireNext(TriggerEvent.OUTPUT_LENGTH_NOT_FULFILLED);
    } else {
      fireNext(TriggerEvent.OUTPUT_LENGTH_FULFILLED);
    }
  }

  void onReceivedEof(Transition<State, TriggerEvent> transition) {
    if (mBytesRead < mLength) {
      fireNext(TriggerEvent.OUTPUT_LENGTH_NOT_FULFILLED);
    } else {
      fireNext(TriggerEvent.OUTPUT_LENGTH_FULFILLED);
    }
  }

  <T extends Throwable> void setException(T e, Transition<State, TriggerEvent> transition) {
    mLastException = e;
    mLastExceptionTrigger = transition.getTrigger();
  }

  /**
   * Adds the exception as a suppressed one of the main exception, typically caught when
   * the client is canceling a request because of the previous main exception. If there is not
   * a main exception, it is ignored.
   */
  <T extends Throwable> void addExceptionAsSuppressed(T suppressed,
      Transition<State, TriggerEvent> transition) {
    if (mLastException != null) {
      mLastException.addSuppressed(suppressed);
    }
  }

  void sendClientCancel() {
    Preconditions.checkNotNull(mChannel, "cannot cancel when channel has not been acquired");
    Protocol.ReadRequest cancelRequest = mRequestBuilder.get().setCancel(true).build();
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(cancelRequest)))
        .addListener((ChannelFutureListener) future -> {
          if (!future.isSuccess()) {
            // Note: cannot call fireNext(TriggerEvent.CHANNEL_ERROR, future.cause()) directly
            // as the callback is called on a Netty I/O thread, it would bypass the blocking queue
            // and create a race condition with the thread the state machine is executing on
            mResponseEventQueue.offer(ResponseEventFactory.getResponseEventFactory()
                .createChannelErrorResponseEvent(future.cause()));
          }
        });
  }

  /**
   * Synchronously closes the channel.
   */
  void syncCloseChannel() {
    Preconditions.checkNotNull(mChannel, "cannot close channel when channel has not been acquired");
    CommonUtils.closeChannel(mChannel);
  }

  // discard data remaining in the queue
  void onClientCancelDataReceived(ByteBuf byteBuf, Transition<State, TriggerEvent> transition) {
    Preconditions.checkState(transition.getTrigger() == TriggerEvent.DATA_AVAILABLE);
    Preconditions.checkState(transition.getSource() == State.CLIENT_CANCEL);
    byteBuf.release();
    fireNext(TriggerEvent.DATA_DISCARDED);
  }

  void onTerminatedExceptionally(Transition<State, TriggerEvent> transition) {
    if (mChannel != null) {
      if (mChannel.isOpen()) {
        CommonUtils.closeChannel(mChannel);
      }
      mContext.releaseNettyChannel(mAddress, mChannel);
    }
  }

  void onTerminatedNormally(Transition<State, TriggerEvent> transition) {
    Preconditions.checkNotNull(mChannel, "terminated normally but channel is null");
    if (mChannel.isOpen()) {
      mChannel.pipeline().removeLast();
      NettyUtils.enableAutoRead(mChannel);
    }
    mContext.releaseNettyChannel(mAddress, mChannel);
  }

  private static boolean tooManyResponseEventsPending(
      BlockingQueue<ResponseEvent> queue, int maxPacketsInFlight) {
    return queue.size() >= maxPacketsInFlight;
  }

  private static class PacketReadHandler extends ChannelInboundHandlerAdapter {
    private final BlockingQueue<ResponseEvent> mResponseEventQueue;
    private final int mMaxPacketsInFlight;

    private final ResponseEventFactory mResponseEventFactory =
        ResponseEventFactory.getResponseEventFactory();

    PacketReadHandler(BlockingQueue<ResponseEvent> responseEventQueue, int maxPacketsInFlight) {
      mResponseEventQueue = responseEventQueue;
      mMaxPacketsInFlight = maxPacketsInFlight;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
      // Precondition check is not used here to avoid calling msg.getClass().getCanonicalName()
      // all the time.
      if (!(msg instanceof RPCProtoMessage)) {
        throw new IllegalStateException(String
            .format("Incorrect response type %s, %s.", msg.getClass().getCanonicalName(), msg));
      }

      ResponseEvent responseEvent;
      RPCProtoMessage rpcProtoMessage = (RPCProtoMessage) msg;
      ProtoMessage message = rpcProtoMessage.getMessage();
      if (message.isReadResponse()) {
        Preconditions.checkState(
            message.asReadResponse().getType() == Protocol.ReadResponse.Type.UFS_READ_HEARTBEAT);
        responseEvent = mResponseEventFactory.createUfsReadHeartBeatResponseEvent();
      } else if (message.isResponse()) {
        Protocol.Response response = message.asResponse();
        // Canceled is considered a valid status and handled in the reader. We avoid creating a
        // CanceledException as an optimization.
        switch (response.getStatus()) {
          case CANCELLED:
            responseEvent = mResponseEventFactory.createCancelResponseEvent();
            break;
          case OK:
            DataBuffer dataBuffer = rpcProtoMessage.getPayloadDataBuffer();
            if (dataBuffer != null) {
              Preconditions.checkState(dataBuffer.getNettyOutput() instanceof ByteBuf,
                  "dataBuffer.getNettyOutput is not of type ByteBuf");
              // no need to retain this buffer since it's already retained by RPCProtoMessage.decode
              responseEvent = mResponseEventFactory.createDataResponseEvent(dataBuffer);
            } else {
              // an empty response indicates the worker has done sending data
              responseEvent = mResponseEventFactory.createEofResponseEvent();
            }
            break;
          default:
            Status status = ProtoUtils.fromProto(response.getStatus());
            AlluxioStatusException error = AlluxioStatusException.from(
                status.withDescription(String.format("Error from server %s: %s",
                    ctx.channel().remoteAddress(), response.getMessage())));
            responseEvent = mResponseEventFactory.createServerErrorResponseEvent(error);
        }
      } else {
        throw new IllegalStateException(
            String.format("Incorrect response type %s.", message));
      }

      if (tooManyResponseEventsPending(mResponseEventQueue, mMaxPacketsInFlight)) {
        NettyUtils.disableAutoRead(ctx.channel());
      }
      mResponseEventQueue.offer(responseEvent);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception is caught while reading data from channel {}:",
          ctx.channel(), cause);
      ResponseEvent responseEvent = mResponseEventFactory.createChannelErrorResponseEvent(cause);
      mResponseEventQueue.offer(responseEvent);
      ctx.fireExceptionCaught(cause);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      LOG.warn("Channel is closed while reading data from channel {}.", ctx.channel());
      ResponseEvent responseEvent = mResponseEventFactory.createChannelErrorResponseEvent(
          new UnavailableException(String.format("Channel %s is closed.", ctx.channel())));
      mResponseEventQueue.offer(responseEvent);
      ctx.fireChannelUnregistered();
    }
  }
}
