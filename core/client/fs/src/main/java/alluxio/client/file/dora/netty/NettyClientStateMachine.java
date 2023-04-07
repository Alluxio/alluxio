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

import static alluxio.client.file.dora.netty.NettyClientStateMachine.Payload.Type.CANCEL;
import static alluxio.client.file.dora.netty.NettyClientStateMachine.Payload.Type.CHANNEL_ERROR;
import static alluxio.client.file.dora.netty.NettyClientStateMachine.Payload.Type.DATA;
import static alluxio.client.file.dora.netty.NettyClientStateMachine.Payload.Type.EOF;
import static alluxio.client.file.dora.netty.NettyClientStateMachine.Payload.Type.HEART_BEAT;
import static alluxio.client.file.dora.netty.NettyClientStateMachine.Payload.Type.SERVER_ERROR;
import static alluxio.client.file.dora.netty.PartialReadException.CauseType;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.CancelledException;
import alluxio.exception.status.UnavailableException;
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
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
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
public class NettyClientStateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(NettyClientStateMachine.class);
  private final StateMachine<State, Trigger> mStateMachine;
  private final Triggers mTriggers;
  private final AtomicReference<Runnable> mNextTrigger = new AtomicReference<>();
  private final FileSystemContext mContext;
  private final long mReadTimeoutMs;
  private final int mMaxPacketsInFlight;
  private final WorkerNetAddress mAddress;
  private final Supplier<Protocol.ReadRequest.Builder> mRequestBuilder;
  private final long mOffset;
  private final int mLength;
  private final WritableByteChannel mOutputChannel;
  private final BlockingQueue<Payload<?>> mQueue = new LinkedBlockingQueue<>();

  @Nullable
  private Channel mChannel;
  private int mBytesRead;
  @Nullable
  private Throwable mLastException;
  @Nullable
  private Trigger mLastExceptionTrigger;

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

  enum Trigger {
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

  private static class Triggers {
    final TriggerWithParameters1<IOException, Trigger> mChannelUnavailable;
    final TriggerWithParameters1<ByteBuf, Trigger> mDataAvailable;
    final TriggerWithParameters1<AlluxioStatusException, Trigger> mServerError;
    final TriggerWithParameters1<Throwable, Trigger> mChannelError;
    final TriggerWithParameters1<IOException, Trigger> mOutputError;
    final TriggerWithParameters1<TimeoutException, Trigger> mTimeout;
    final TriggerWithParameters1<InterruptedException, Trigger> mInterrupted;

    public Triggers(StateMachineConfig<State, Trigger> config) {
      mChannelUnavailable =
          config.setTriggerParameters(Trigger.CHANNEL_UNAVAILABLE, IOException.class);
      mDataAvailable =
          config.setTriggerParameters(Trigger.DATA_AVAILABLE, ByteBuf.class);
      mServerError =
          config.setTriggerParameters(Trigger.SERVER_ERROR, AlluxioStatusException.class);
      mChannelError =
          config.setTriggerParameters(Trigger.CHANNEL_ERROR, Throwable.class);
      mOutputError =
          config.setTriggerParameters(Trigger.OUTPUT_ERROR, IOException.class);
      mTimeout =
          config.setTriggerParameters(Trigger.TIMEOUT, TimeoutException.class);
      mInterrupted =
          config.setTriggerParameters(Trigger.INTERRUPTED, InterruptedException.class);
    }
  }

  /**
   * Constructor.
   *
   * @param context
   * @param address
   * @param requestBuilder
   * @param outChannel
   */
  public NettyClientStateMachine(FileSystemContext context, WorkerNetAddress address,
      Protocol.ReadRequest.Builder requestBuilder, WritableByteChannel outChannel) {
    mContext = context;
    AlluxioConfiguration conf = context.getClusterConf();
    mReadTimeoutMs = conf.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
    mMaxPacketsInFlight = conf.getInt(PropertyKey.USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS);
    mAddress = address;
    // clone the builder so that the initial values does not get overridden
    mRequestBuilder = requestBuilder::clone;
    mOffset = requestBuilder.getOffset();
    mLength = (int) requestBuilder.getLength();
    mOutputChannel = outChannel;

    StateMachineConfig<State, Trigger> config = new StateMachineConfig<>();
    mTriggers = new Triggers(config);

    config.configure(State.CREATED)
        .permit(Trigger.START, State.ACQUIRING_CHANNEL);
    config.configure(State.ACQUIRING_CHANNEL)
        .onEntry(this::acquireNettyChannel)
        .permit(Trigger.CHANNEL_AVAILABLE, State.CHANNEL_ACTIVE, this::sendRequest)
        .permit(Trigger.CHANNEL_UNAVAILABLE, State.TERMINATED_EXCEPTIONALLY); // no need to cancel
    config.configure(State.CHANNEL_ACTIVE)
        .onEntry(this::pollResponseFromQueue)
        .permit(Trigger.DATA_AVAILABLE, State.RECEIVED_DATA)
        .permitReentry(Trigger.HEART_BEAT)
        .permit(Trigger.EOF, State.RECEIVED_EOF)
        .permit(Trigger.TIMEOUT, State.CLIENT_CANCEL, this::sendClientCancel)
        .permit(Trigger.INTERRUPTED, State.CLIENT_CANCEL, this::sendClientCancel)
        .permit(Trigger.SERVER_ERROR, State.CLIENT_CANCEL, this::sendClientCancel)
        .permit(Trigger.CHANNEL_ERROR, State.CLIENT_CANCEL, this::sendClientCancel);
    config.configure(State.RECEIVED_DATA)
        .onEntryFrom(mTriggers.mDataAvailable, this::onReceivedData)
        .permit(Trigger.OUTPUT_ERROR, State.CLIENT_CANCEL, this::sendClientCancel)
        .permit(Trigger.OUTPUT_LENGTH_FULFILLED, State.EXPECTING_EOF)
        .permit(Trigger.OUTPUT_LENGTH_NOT_FULFILLED, State.CHANNEL_ACTIVE);
    config.configure(State.EXPECTING_EOF)
        .onEntry(this::pollResponseFromQueue)
        .permit(Trigger.EOF, State.TERMINATED_NORMALLY)
        // todo(bowen): do we need to handle DATA_AVAILABLE from an insane server?
        // we have got enough data to exit correctly,
        // so just close the channel if anything unexpected happens instead of throwing an error
        .permit(Trigger.TIMEOUT, State.TERMINATED_NORMALLY, this::syncCloseChannel)
        .permit(Trigger.INTERRUPTED, State.TERMINATED_NORMALLY, this::syncCloseChannel)
        .permit(Trigger.SERVER_ERROR, State.TERMINATED_NORMALLY, this::syncCloseChannel)
        .permit(Trigger.CHANNEL_ERROR, State.TERMINATED_NORMALLY, this::syncCloseChannel);
    config.configure(State.RECEIVED_EOF)
        .onEntry(this::onReceivedEof)
        .permit(Trigger.OUTPUT_LENGTH_FULFILLED, State.TERMINATED_NORMALLY)
        .permit(Trigger.OUTPUT_LENGTH_NOT_FULFILLED, State.TERMINATED_NORMALLY);
    config.configure(State.CLIENT_CANCEL)
        .onEntryFrom(mTriggers.mInterrupted, this::setException)
        .onEntryFrom(mTriggers.mTimeout, this::setException)
        .onEntryFrom(mTriggers.mServerError, this::setException)
        .onEntryFrom(mTriggers.mChannelError, this::setException)
        .onEntryFrom(mTriggers.mOutputError, this::setException)
        .onEntry(this::pollResponseFromQueue)
        .permit(Trigger.DATA_AVAILABLE, State.CLIENT_CANCEL_DATA_RECEIVED)
        .permitReentry(Trigger.EOF)
        .permitReentry(Trigger.HEART_BEAT)
        // this is the good case where server has acknowledged client cancel
        // and the channel is OK for reuse
        .permit(Trigger.SERVER_CANCEL, State.TERMINATED_NORMALLY)
        .permit(Trigger.INTERRUPTED, State.TERMINATED_EXCEPTIONALLY)
        .permit(Trigger.TIMEOUT, State.TERMINATED_EXCEPTIONALLY)
        .permit(Trigger.SERVER_ERROR, State.TERMINATED_EXCEPTIONALLY)
        .permit(Trigger.CHANNEL_ERROR, State.TERMINATED_EXCEPTIONALLY);
    config.configure(State.TERMINATED_EXCEPTIONALLY)
        .substateOf(State.TERMINATED)
        .onEntryFrom(mTriggers.mChannelUnavailable, this::setException)
        .onEntryFrom(mTriggers.mInterrupted, this::addExceptionAsSuppressed)
        .onEntryFrom(mTriggers.mTimeout, this::addExceptionAsSuppressed)
        .onEntryFrom(mTriggers.mServerError, this::addExceptionAsSuppressed)
        .onEntryFrom(mTriggers.mChannelError, this::addExceptionAsSuppressed)
        .onEntry(this::onTerminatedExceptionally);
    config.configure(State.CLIENT_CANCEL_DATA_RECEIVED)
        .onEntryFrom(mTriggers.mDataAvailable, this::onClientCancelDataReceived)
        .permit(Trigger.DATA_DISCARDED, State.CLIENT_CANCEL);
    config.configure(State.TERMINATED_NORMALLY)
        .substateOf(State.TERMINATED)
        .onEntry(this::onTerminatedNormally);

    mStateMachine = new StateMachine<>(/* initialState */ State.CREATED, config);
    mStateMachine.setTrace(new DebugLoggingTracer<>(LOG));
    mStateMachine.fireInitialTransition();
  }

  /**
   * Starts the state machine.
   */
  public void run() {
    Preconditions.checkState(mStateMachine.isInState(State.CREATED),
        "state machine cannot be restarted: expected initial state %s, encountered %s",
        State.CREATED, mStateMachine.getState());
    fireNext(Trigger.START);
    try {
      for (Runnable trigger = mNextTrigger.getAndSet(null);
           trigger != null;
           trigger = mNextTrigger.getAndSet(null)) {
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

  /**
   * Helper method to allow firing triggers within state handler methods.
   * If the triggers are fired directly within state handler methods, they will likely make
   * recursive calls and blow up the stack.
   * @param trigger the next trigger to fire
   */
  void fireNext(Trigger trigger) {
    mNextTrigger.set(() -> mStateMachine.fire(trigger));
  }

  <Arg0T> void fireNext(TriggerWithParameters1<Arg0T, Trigger> trigger, Arg0T arg0) {
    mNextTrigger.set(() -> mStateMachine.fire(trigger, arg0));
  }

  void acquireNettyChannel() {
    try {
      mChannel = mContext.acquireNettyChannel(mAddress);
      mChannel.pipeline().addLast(new PacketReadHandler(mQueue, mMaxPacketsInFlight));
    } catch (IOException ioe) {
      fireNext(mTriggers.mChannelUnavailable, ioe);
      return;
    }
    fireNext(Trigger.CHANNEL_AVAILABLE);
  }

  void sendRequest() {
    Preconditions.checkNotNull(mChannel, "channel has not been acquired");
    Protocol.ReadRequest readRequest = mRequestBuilder.get().clearCancel().build();
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(readRequest)))
        .addListener((ChannelFutureListener) future -> {
          if (!future.isSuccess()) {
            // Note: cannot call fireNext(Trigger.CHANNEL_ERROR, future.cause()) directly
            // as the callback is called on a Netty I/O thread, it would bypass the blocking queue
            // and create a race condition with the thread the state machine is executing on
            mQueue.offer(Payload.channelError(future.cause()));
          }
        });
  }

  void pollResponseFromQueue() {
    if (!tooManyPacketsPending(mQueue, mMaxPacketsInFlight)) {
      NettyUtils.enableAutoRead(mChannel);
    }
    Payload<?> payload;
    try {
      payload = mQueue.poll(mReadTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      fireNext(mTriggers.mInterrupted, interruptedException);
      return;
    }
    // todo(bowen): make this a visitor pattern
    // todo(bowen): find a way to do exhaustive enum matching and get rid of the UNKNOWN_PAYLOAD
    if (payload == null) {
      fireNext(mTriggers.mTimeout, new TimeoutException(
          "Timed out when waiting for server response for " + mReadTimeoutMs + " ms"));
    } else if (payload.type() == DATA) {
      fireNext(mTriggers.mDataAvailable, payload.payload(DATA));
    } else if (payload.type() == CANCEL) {
      fireNext(Trigger.SERVER_CANCEL);
    } else if (payload.type() == EOF) {
      fireNext(Trigger.EOF);
    } else if (payload.type() == SERVER_ERROR) {
      fireNext(mTriggers.mServerError, payload.payload(SERVER_ERROR));
    } else if (payload.type() == CHANNEL_ERROR) {
      fireNext(mTriggers.mChannelError, payload.payload(CHANNEL_ERROR));
    } else if (payload.type() == HEART_BEAT) {
      fireNext(Trigger.HEART_BEAT);
    } else {
      fireNext(Trigger.UNKNOWN_PAYLOAD);
    }
  }

  void onReceivedData(ByteBuf buf, Transition<State, Trigger> transition) {
    Preconditions.checkState(Trigger.DATA_AVAILABLE == transition.getTrigger());
    ByteBuf byteBuf = buf.slice();
    int readableBytes = byteBuf.readableBytes();
    int sliceEnd = Math.min(readableBytes, mLength - mBytesRead);
    // todo(bowen): handle case where ByteBuf does not support getting a bytebuffer
    ByteBuffer toWrite = byteBuf.nioBuffer(0, sliceEnd);
    try {
      mBytesRead += mOutputChannel.write(toWrite);
    } catch (IOException ioe) {
      fireNext(mTriggers.mOutputError, ioe);
      return;
    } finally {
      // previously retained in packet read handler
      byteBuf.release();
    }
    if (mBytesRead < mLength) {
      fireNext(Trigger.OUTPUT_LENGTH_NOT_FULFILLED);
    } else {
      fireNext(Trigger.OUTPUT_LENGTH_FULFILLED);
    }
  }

  void onReceivedEof(Transition<State, Trigger> transition) {
    if (mBytesRead < mLength) {
      fireNext(Trigger.OUTPUT_LENGTH_NOT_FULFILLED);
    } else {
      fireNext(Trigger.OUTPUT_LENGTH_FULFILLED);
    }
  }

  <T extends Throwable> void setException(T e, Transition<State, Trigger> transition) {
    mLastException = e;
    mLastExceptionTrigger = transition.getTrigger();
  }

  /**
   * Adds the exception as a suppressed one of the main exception, typically caught when
   * the client is canceling a request because of the previous main exception. If there is not
   * a main exception, it is ignored.
   */
  <T extends Throwable> void addExceptionAsSuppressed(T suppressed,
      Transition<State, Trigger> transition) {
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
            // Note: cannot call fireNext(Trigger.CHANNEL_ERROR, future.cause()) directly
            // as the callback is called on a Netty I/O thread, it would bypass the blocking queue
            // and create a race condition with the thread the state machine is executing on
            mQueue.offer(Payload.channelError(future.cause()));
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
  void onClientCancelDataReceived(ByteBuf byteBuf, Transition<State, Trigger> transition) {
    Preconditions.checkState(transition.getTrigger() == Trigger.DATA_AVAILABLE);
    Preconditions.checkState(transition.getSource() == State.CLIENT_CANCEL);
    byteBuf.release();
    fireNext(Trigger.DATA_DISCARDED);
  }

  void onTerminatedExceptionally(Transition<State, Trigger> transition) {
    if (mChannel != null) {
      if (mChannel.isOpen()) {
        CommonUtils.closeChannel(mChannel);
      }
      mContext.releaseNettyChannel(mAddress, mChannel);
    }
  }

  void onTerminatedNormally(Transition<State, Trigger> transition) {
    Preconditions.checkNotNull(mChannel, "terminated normally but channel is null");
    if (mChannel.isOpen()) {
      mChannel.pipeline().removeLast();
      NettyUtils.enableAutoRead(mChannel);
    }
    mContext.releaseNettyChannel(mAddress, mChannel);
  }

  private static boolean tooManyPacketsPending(
      BlockingQueue<Payload<?>> queue, int maxPacketsInFlight) {
    return queue.size() >= maxPacketsInFlight;
  }

  private static class PacketReadHandler extends ChannelInboundHandlerAdapter {
    private final BlockingQueue<Payload<?>> mPackets;
    private final int mMaxPacketsInFlight;

    PacketReadHandler(BlockingQueue<Payload<?>> queue, int maxPacketsInFlight) {
      mPackets = queue;
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

      Payload<?> payload;
      RPCProtoMessage rpcProtoMessage = (RPCProtoMessage) msg;
      ProtoMessage message = rpcProtoMessage.getMessage();
      if (message.isReadResponse()) {
        Preconditions.checkState(
            message.asReadResponse().getType() == Protocol.ReadResponse.Type.UFS_READ_HEARTBEAT);
        payload = Payload.ufsReadHeartBeat();
      } else if (message.isResponse()) {
        Protocol.Response response = message.asResponse();
        // Canceled is considered a valid status and handled in the reader. We avoid creating a
        // CanceledException as an optimization.
        switch (response.getStatus()) {
          case CANCELLED:
            payload = Payload.cancel(
                new CancelledException("Server canceled: " + response.getMessage()));
            break;
          case OK:
            DataBuffer dataBuffer = rpcProtoMessage.getPayloadDataBuffer();
            if (dataBuffer != null) {
              Preconditions.checkState(dataBuffer.getNettyOutput() instanceof ByteBuf,
                  "dataBuffer.getNettyOutput is not of type ByteBuf");
              ByteBuf data = (ByteBuf) dataBuffer.getNettyOutput();
              // no need to retain this buffer since it's already retained by RPCProtoMessage.decode
              payload = Payload.data(data);
            } else {
              // an empty response indicates the worker has done sending data
              payload = Payload.eof();
            }
            break;
          default:
            Status status = ProtoUtils.fromProto(response.getStatus());
            AlluxioStatusException error = AlluxioStatusException.from(
                status.withDescription(String.format("Error from server %s: %s",
                    ctx.channel().remoteAddress(), response.getMessage())));
            payload = Payload.serverError(error);
        }
      } else {
        throw new IllegalStateException(
            String.format("Incorrect response type %s.", message));
      }

      if (tooManyPacketsPending(mPackets, mMaxPacketsInFlight)) {
        NettyUtils.disableAutoRead(ctx.channel());
      }
      mPackets.offer(payload);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception is caught while reading data from channel {}:",
          ctx.channel(), cause);
      Payload<?> payload = Payload.channelError(cause);
      mPackets.offer(payload);
      ctx.fireExceptionCaught(cause);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      LOG.warn("Channel is closed while reading data from channel {}.", ctx.channel());
      Payload<?> payload = Payload.channelError(
          new UnavailableException(String.format("Channel %s is closed.", ctx.channel())));
      mPackets.offer(payload);
      ctx.fireChannelUnregistered();
    }
  }

  static class Payload<T extends Payload.Type<?>> {
    interface Type<P> {
      Class<P> payloadType();

      /** A packet containing data. */
      Data DATA = new Data();
      /** A packet containing a heart beat from server. */
      UfsReadHeartBeat HEART_BEAT = new UfsReadHeartBeat();
      /** The EOF message. */
      Eof EOF = new Eof();
      /** The cancel reply from server, in acknowledge to a client cancel message. */
      Cancel CANCEL = new Cancel();
      /**
       * The server rejected the client due to a bad request, encountered an internal error, etc.,
       * and sent an error response,
       * but the channel is otherwise not affected and should be good for reuse.
       */
      ServerError SERVER_ERROR = new ServerError();
      /**
       * There is an error on the channel, may be an exception from pipeline handlers,
       * or the channel has been closed, etc. The channel is probably not good for reuse any more.
       */
      ChannelError CHANNEL_ERROR = new ChannelError();
    }

    static class Data implements Type<ByteBuf> {
      @Override
      public Class<ByteBuf> payloadType() {
        return ByteBuf.class;
      }
    }

    static class UfsReadHeartBeat implements Type<Void> {
      @Override
      public Class<Void> payloadType() {
        return Void.TYPE;
      }
    }

    static class Eof implements Type<Void> {
      @Override
      public Class<Void> payloadType() {
        return Void.TYPE;
      }
    }

    static class Cancel implements Type<CancelledException> {
      @Override
      public Class<CancelledException> payloadType() {
        return CancelledException.class;
      }
    }

    static class ServerError implements Type<AlluxioStatusException> {
      @Override
      public Class<AlluxioStatusException> payloadType() {
        return AlluxioStatusException.class;
      }
    }

    static class ChannelError implements Type<Throwable> {
      @Override
      public Class<Throwable> payloadType() {
        return Throwable.class;
      }
    }

    private final T mType;
    private final Object mPayload;

    private Payload(T type, Object payload) {
      Preconditions.checkArgument((type.payloadType() == Void.TYPE && payload == null)
          || type.payloadType().isInstance(payload));
      mType = type;
      mPayload = payload;
    }

    static Payload<Data> data(ByteBuf buf) {
      return new Payload<>(Type.DATA, buf);
    }

    static Payload<UfsReadHeartBeat> ufsReadHeartBeat() {
      return new Payload<>(HEART_BEAT, null);
    }

    static Payload<Eof> eof() {
      return new Payload<>(EOF, null);
    }

    static Payload<Cancel> cancel(CancelledException exception) {
      return new Payload<>(CANCEL, exception);
    }

    static Payload<ServerError> serverError(AlluxioStatusException error) {
      return new Payload<>(SERVER_ERROR, error);
    }

    static Payload<ChannelError> channelError(Throwable error) {
      return new Payload<>(CHANNEL_ERROR, error);
    }

    public T type() {
      return mType;
    }

    public <P> P payload(Type<P> type) {
      Preconditions.checkArgument(type == mType, "payload type mismatch");
      Class<P> clazz = type.payloadType();
      return clazz.cast(mPayload);
    }
  }
}
