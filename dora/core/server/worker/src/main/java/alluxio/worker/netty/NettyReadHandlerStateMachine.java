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
import alluxio.metrics.MultiDimensionalMetricsSystem;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.util.CommonUtils;
import alluxio.worker.netty.AbstractReadHandler.PacketReader;

import com.github.oxo42.stateless4j.StateMachine;
import com.github.oxo42.stateless4j.StateMachineConfig;
import com.github.oxo42.stateless4j.transitions.Transition;
import com.github.oxo42.stateless4j.triggers.TriggerWithParameters1;
import com.github.oxo42.stateless4j.triggers.TriggerWithParameters2;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.grpc.Status;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 *  State machine of Netty Server in Alluxio Worker.
 *  <br>
 *  You can get a diagram describing the state transitions of this state machine, by calling
 *  {@link #generateStateDiagram(Path)}. After you update the states and triggers, create a diagram
 *  and check if the new state transitions are properly handled.
 *
 * @param <ReqT> the ReadRequest type
 */
public class NettyReadHandlerStateMachine<ReqT extends ReadRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(NettyReadHandlerStateMachine.class);
  private static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS);

  private final Class<ReqT> mRequestType;
  private final StateMachine<State, TriggerEvent> mStateMachine;

  private final AtomicReference<Runnable> mNextTriggerEvent = new AtomicReference<>();

  private final LinkedBlockingQueue<ChannelEvent> mChannelEventQueue =
      new LinkedBlockingQueue<>();

  private final PacketReader.Factory<ReqT, ? extends PacketReader<ReqT>> mPacketReaderFactory;

  private final TriggerEventsWithParam mTriggerEventsWithParam;

  private final Channel mChannel;

  private interface ChannelEvent {
    // tag interface
  }

  private interface ChannelEventVisitor<ReqT extends ReadRequest> {

    boolean visit(ReadRequestReceived<ReqT> channelEvent);

    boolean visit(WriteFutureResolved channelEvent);

    boolean visit(ChannelErrorOccurred channelEvent);

    boolean visit(ChannelDisconnected channelEvent);

    boolean visit(ClientCanceled channelEvent);
  }

  private boolean pollAndHandleChannelEvent(ChannelEventVisitor<ReqT> visitor) {
    ChannelEvent channelEvent = mChannelEventQueue.poll();
    if (channelEvent == null) {
      return false;
    }
    return handleChannelEvent(channelEvent, visitor);
  }

  private boolean takeAndHandleChannelEvent(ChannelEventVisitor<ReqT> visitor) {
    try {
      ChannelEvent channelEvent = mChannelEventQueue.take();
      return handleChannelEvent(channelEvent, visitor);
    } catch (InterruptedException interruptedException) {
      fireNext(mTriggerEventsWithParam.mInterrupted, interruptedException);
      return true;
    }
  }

  private boolean takeAndHandleChannelEvent(
      RequestContext requestContext, ChannelEventVisitor<ReqT> visitor) {
    try {
      ChannelEvent channelEvent = mChannelEventQueue.take();
      return handleChannelEvent(channelEvent, visitor);
    } catch (InterruptedException interruptedException) {
      fireNext(mTriggerEventsWithParam.mInterruptedDuringReq, requestContext, interruptedException);
      return true;
    }
  }

  private boolean handleChannelEvent(ChannelEvent channelEvent, ChannelEventVisitor<ReqT> visitor) {
    if (channelEvent instanceof ReadRequestReceived) {
      @SuppressWarnings("unchecked")
      ReadRequestReceived<ReqT> readRequestReceived = (ReadRequestReceived<ReqT>) channelEvent;
      return visitor.visit(readRequestReceived);
    } else if (channelEvent instanceof WriteFutureResolved) {
      return visitor.visit((WriteFutureResolved) channelEvent);
    } else if (channelEvent instanceof ChannelErrorOccurred) {
      return visitor.visit((ChannelErrorOccurred) channelEvent);
    } else if (channelEvent instanceof ChannelDisconnected) {
      return visitor.visit((ChannelDisconnected) channelEvent);
    } else if (channelEvent instanceof ClientCanceled) {
      return visitor.visit((ClientCanceled) channelEvent);
    } else {
      // unhandled channel event, this is a programming error
      throw new IllegalStateException(
          String.format("Unhandled channel event %s", channelEvent.getClass()));
    }
  }

  private static class ReadRequestReceived<ReqT extends ReadRequest> implements ChannelEvent {
    private final ReqT mRequest;

    public ReadRequestReceived(ReqT request) {
      mRequest = request;
    }

    public ReqT getRequest() {
      return mRequest;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("mRequest", mRequest)
          .toString();
    }
  }

  private static class WriteFutureResolved implements ChannelEvent {
    private final int mBytesWritten;
    private final Throwable mFailure;

    private WriteFutureResolved(int bytesWritten, Throwable cause) {
      mBytesWritten = bytesWritten;
      mFailure = cause;
    }

    public boolean isSuccess() {
      return mFailure == null;
    }

    public Throwable getCause() {
      return mFailure;
    }

    public int getBytesWritten() {
      return mBytesWritten;
    }

    public static WriteFutureResolved success(int bytesWritten) {
      return new WriteFutureResolved(bytesWritten, null);
    }

    public static WriteFutureResolved failure(Throwable cause) {
      Preconditions.checkNotNull(cause, "cause");
      return new WriteFutureResolved(0, cause);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("mBytesWritten", mBytesWritten)
          .add("mFailure", mFailure)
          .toString();
    }
  }

  private static class ChannelErrorOccurred implements ChannelEvent {
    private final Throwable mException;

    private ChannelErrorOccurred(Throwable throwable) {
      mException = throwable;
    }

    public Throwable getCause() {
      return mException;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("mException", mException)
          .toString();
    }
  }

  private static class ChannelDisconnected implements ChannelEvent {
    private ChannelDisconnected() { }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .toString();
    }
  }

  private static class ClientCanceled implements ChannelEvent {
    private ClientCanceled() { }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .toString();
    }
  }

  private static class RequestContext {
    /*
     * Invariants:
     *     |-------------->|------------>|------------>|
     * req.start  <=  posWritten  <=  posRead  <=  req.end
     *
     * When request starts:
     *   req.start == posWritten == posRead
     */
    private long mPosRead;
    private long mPosWritten;
    private final Class<? extends ReadRequest> mRequestType;
    private final ReadRequest mRequest;
    private final PacketReader<? extends ReadRequest> mPacketReader;

    private RequestContext(long positionRead, long positionWritten,
        Class<? extends ReadRequest> requestType, ReadRequest request,
        PacketReader<? extends ReadRequest> packetReader) {
      Preconditions.checkArgument(request.getStart() <= positionWritten
          && positionWritten <= positionRead
          && positionRead <= request.getEnd(),
          "Invalid task progress, posRead: %s, posWritten: %s, request start: %s, request end: %s",
          positionRead, positionWritten, request.getStart(), request.getEnd());
      mPosRead = positionRead;
      mPosWritten = positionWritten;
      mRequestType = requestType;
      mRequest = request;
      mPacketReader = packetReader;
    }

    public static <ReqT extends ReadRequest> RequestContext createForNewRequest(
        Class<ReqT> requestType, ReqT request, PacketReader<ReqT> packetReader) {
      return new RequestContext(request.getStart(), request.getStart(), requestType, request,
          packetReader);
    }

    public void increaseReadProgress(long increment) {
      Preconditions.checkArgument(increment <= mRequest.getEnd() - mPosRead,
          "read progress cannot go past request end: %s, current read pos: %s, increment: %s",
          mRequest.getEnd(), mPosRead, increment);
      mPosRead += increment;
      // TODO(bowen): add metrics accounting here?
    }

    public void increaseWriteProgress(long increment) {
      Preconditions.checkArgument(increment <= mPosRead - mPosWritten,
          "write progress cannot go past read position: %s, current written pos: %s, increment: %s",
          mPosRead, mPosWritten, increment);
      mPosWritten += increment;
    }

    public long bytesPending() {
      return positionRead() - positionWritten();
    }

    public long positionRead() {
      return mPosRead;
    }

    public long positionWritten() {
      return mPosWritten;
    }

    public long positionStart() {
      return mRequest.getStart();
    }

    public long positionEnd() {
      return mRequest.getEnd();
    }

    public ReadRequest getRequest() {
      return mRequest;
    }

    public <ReqT extends ReadRequest> PacketReader<ReqT> getPacketReader(Class<ReqT> requestType) {
      Preconditions.checkArgument(requestType == mRequestType);
      @SuppressWarnings("unchecked")
      PacketReader<ReqT> packetReader = (PacketReader<ReqT>) mPacketReader;
      return packetReader;
    }
  }

  enum TriggerEvent {
    UNEXPECTED_CLIENT_MESSAGE,
    UNEXPECTED_CLIENT_MESSAGE_DURING_REQUEST,
    START,
    REQUEST_RECEIVED,
    PACKET_READER_CREATION_ERROR,
    CHANNEL_CLOSED,
    CHANNEL_CLOSED_DURING_REQUEST,
    CANCELLED,
    INTERRUPTED,
    INTERRUPTED_DURING_REQUEST,
    DATA_AVAILABLE,
    TOO_MANY_PENDING_CHUNKS,
    RESUME,
    OUTPUT_LENGTH_NOT_FULFILLED,
    OUTPUT_LENGTH_FULFILLED,
    CHANNEL_EXCEPTION,
    CHANNEL_EXCEPTION_DURING_REQUEST,
    READ_DATA_ERROR,
    SEND_DATA_ERROR,
    REQUEST_COMPLETION_ERROR,
    REPLY_MESSAGE_DONE,
    REPLY_MESSAGE_ERROR,
    COMPLETE_REQUEST,
    REQUEST_COMPLETED
  }

  enum State {
    CREATED,
    CHANNEL_IDLE,
    READING_DATA,
    SENDING_DATA,
    EOF,
    PAUSED,
    CLIENT_CANCEL,
    ERROR_OUTSIDE_REQUEST,
    ERROR_DURING_REQUEST,
    COMPLETING_REQUEST,
    TERMINATED,
    TERMINATED_NORMALLY,
    TERMINATED_EXCEPTIONALLY,
  }

  /**
   * Trigger events with parameters.
   */
  private class TriggerEventsWithParam {
    public final TriggerWithParameters1<ChannelEvent, TriggerEvent> mUnexpectedClientMessage;
    public final TriggerWithParameters2<RequestContext, ChannelEvent, TriggerEvent>
        mUnexpectedClientMessageDuringReq;
    public final TriggerWithParameters1<ReqT, TriggerEvent> mRequestReceived;
    public final TriggerWithParameters1<Exception, TriggerEvent> mPacketReaderCreationError;
    public final TriggerWithParameters2<RequestContext, DataBuffer, TriggerEvent> mDataAvailable;
    public final TriggerWithParameters1<RequestContext, TriggerEvent> mOutputLengthFulfilled;
    public final TriggerWithParameters1<RequestContext, TriggerEvent> mOutputLengthNotFulfilled;
    public final TriggerWithParameters1<RequestContext, TriggerEvent> mClientCancelled;
    public final TriggerWithParameters1<RequestContext, TriggerEvent> mTooManyPendingPackets;
    public final TriggerWithParameters1<RequestContext, TriggerEvent> mResume;
    public final TriggerWithParameters2<RequestContext, IOException, TriggerEvent> mReadDataError;
    public final TriggerWithParameters2<RequestContext, Throwable, TriggerEvent> mSendDataError;
    public final TriggerWithParameters1<Throwable, TriggerEvent> mReplyMessageError;
    public final TriggerWithParameters1<Throwable, TriggerEvent> mChannelException;
    public final TriggerWithParameters2<RequestContext, Throwable, TriggerEvent>
        mChannelExceptionDuringReq;
    public final TriggerWithParameters1<RequestContext, TriggerEvent> mChannelClosedDuringReq;

    public final TriggerWithParameters1<InterruptedException, TriggerEvent> mInterrupted;
    public final TriggerWithParameters2<RequestContext, InterruptedException, TriggerEvent>
        mInterruptedDuringReq;
    public final TriggerWithParameters1<RequestContext, TriggerEvent> mCompleteRequest;
    public final TriggerWithParameters1<Throwable, TriggerEvent> mRequestCompletionError;

    /**
     * Trigger event with parameters for @{NettyClientStateMachine}.
     *
     * @param config the {@code StateMachineConfig} for binding trigger event with parameter
     *               for {@code StateMachine}
     */
    public TriggerEventsWithParam(Class<ReqT> requestType,
        StateMachineConfig<State, TriggerEvent> config) {
      mUnexpectedClientMessage =
          config.setTriggerParameters(TriggerEvent.UNEXPECTED_CLIENT_MESSAGE, ChannelEvent.class);
      mUnexpectedClientMessageDuringReq =
          config.setTriggerParameters(TriggerEvent.UNEXPECTED_CLIENT_MESSAGE_DURING_REQUEST,
              RequestContext.class, ChannelEvent.class);
      mRequestReceived =
          config.setTriggerParameters(TriggerEvent.REQUEST_RECEIVED, requestType);
      mPacketReaderCreationError =
          config.setTriggerParameters(TriggerEvent.PACKET_READER_CREATION_ERROR,
              Exception.class);
      mDataAvailable = config.setTriggerParameters(TriggerEvent.DATA_AVAILABLE,
          RequestContext.class, DataBuffer.class);
      mOutputLengthFulfilled =
          config.setTriggerParameters(TriggerEvent.OUTPUT_LENGTH_FULFILLED, RequestContext.class);
      mOutputLengthNotFulfilled =
          config.setTriggerParameters(TriggerEvent.OUTPUT_LENGTH_NOT_FULFILLED,
              RequestContext.class);
      mClientCancelled =
          config.setTriggerParameters(TriggerEvent.CANCELLED, RequestContext.class);
      mTooManyPendingPackets =
          config.setTriggerParameters(TriggerEvent.TOO_MANY_PENDING_CHUNKS, RequestContext.class);
      mResume =
          config.setTriggerParameters(TriggerEvent.RESUME, RequestContext.class);
      mReadDataError =
          config.setTriggerParameters(TriggerEvent.READ_DATA_ERROR,
              RequestContext.class, IOException.class);
      mChannelException =
          config.setTriggerParameters(TriggerEvent.CHANNEL_EXCEPTION, Throwable.class);
      mChannelExceptionDuringReq =
          config.setTriggerParameters(TriggerEvent.CHANNEL_EXCEPTION_DURING_REQUEST,
              RequestContext.class, Throwable.class);
      mChannelClosedDuringReq =
          config.setTriggerParameters(TriggerEvent.CHANNEL_CLOSED_DURING_REQUEST,
              RequestContext.class);
      mInterrupted =
          config.setTriggerParameters(TriggerEvent.INTERRUPTED, InterruptedException.class);
      mInterruptedDuringReq =
          config.setTriggerParameters(TriggerEvent.INTERRUPTED_DURING_REQUEST,
              RequestContext.class, InterruptedException.class);
      mSendDataError =
          config.setTriggerParameters(TriggerEvent.SEND_DATA_ERROR,
              RequestContext.class, Throwable.class);
      mReplyMessageError =
          config.setTriggerParameters(TriggerEvent.REPLY_MESSAGE_ERROR, Throwable.class);
      mCompleteRequest =
          config.setTriggerParameters(TriggerEvent.COMPLETE_REQUEST, RequestContext.class);
      mRequestCompletionError =
          config.setTriggerParameters(TriggerEvent.REQUEST_COMPLETION_ERROR, Throwable.class);
    }
  }

  /**
   * The state machine for reading packet task in Alluxio Worker.
   * @param channel the netty channel
   * @param requestType the type of the request class
   * @param packetReaderFactory the packet reader factory for creating packet readers
   */
  public NettyReadHandlerStateMachine(Channel channel, Class<ReqT> requestType,
      PacketReader.Factory<ReqT, ? extends PacketReader<ReqT>> packetReaderFactory) {
    StateMachineConfig<State, TriggerEvent> config = new StateMachineConfig<>();
    mTriggerEventsWithParam = new TriggerEventsWithParam(requestType, config);

    config.configure(State.CREATED)
        .permit(TriggerEvent.START, State.CHANNEL_IDLE);
    config.configure(State.CHANNEL_IDLE)
        .onEntry(this::onChannelIdle)
        .permit(TriggerEvent.REQUEST_RECEIVED, State.READING_DATA)
        .permit(TriggerEvent.UNEXPECTED_CLIENT_MESSAGE, State.ERROR_OUTSIDE_REQUEST)
        .permit(TriggerEvent.INTERRUPTED, State.TERMINATED_EXCEPTIONALLY)
        .permit(TriggerEvent.CHANNEL_EXCEPTION, State.TERMINATED_EXCEPTIONALLY)
        .permit(TriggerEvent.CHANNEL_CLOSED, State.TERMINATED_NORMALLY);
    config.configure(State.READING_DATA)
        .onEntryFrom(mTriggerEventsWithParam.mRequestReceived, this::startNewRequest)
        .onEntryFrom(mTriggerEventsWithParam.mResume, this::readData)
        .onEntryFrom(mTriggerEventsWithParam.mOutputLengthNotFulfilled, this::readData)
        .permit(TriggerEvent.PACKET_READER_CREATION_ERROR, State.ERROR_OUTSIDE_REQUEST)
        .permit(TriggerEvent.OUTPUT_LENGTH_FULFILLED, State.EOF)
        .permit(TriggerEvent.READ_DATA_ERROR, State.ERROR_DURING_REQUEST)
        .permit(TriggerEvent.DATA_AVAILABLE, State.SENDING_DATA);
    config.configure(State.SENDING_DATA)
        .onEntryFrom(mTriggerEventsWithParam.mDataAvailable, this::sendData)
        .permit(TriggerEvent.OUTPUT_LENGTH_NOT_FULFILLED, State.READING_DATA)
        .permit(TriggerEvent.OUTPUT_LENGTH_FULFILLED, State.EOF)
        .permit(TriggerEvent.TOO_MANY_PENDING_CHUNKS, State.PAUSED)
        .permit(TriggerEvent.CANCELLED, State.CLIENT_CANCEL)
        .permit(TriggerEvent.CHANNEL_EXCEPTION_DURING_REQUEST, State.ERROR_DURING_REQUEST)
        .permit(TriggerEvent.SEND_DATA_ERROR, State.ERROR_DURING_REQUEST)
        .permit(TriggerEvent.UNEXPECTED_CLIENT_MESSAGE_DURING_REQUEST, State.ERROR_DURING_REQUEST)
        .permit(TriggerEvent.CHANNEL_CLOSED_DURING_REQUEST, State.COMPLETING_REQUEST);
    config.configure(State.PAUSED)
        .onEntryFrom(mTriggerEventsWithParam.mTooManyPendingPackets, this::onPaused)
        .permit(TriggerEvent.INTERRUPTED_DURING_REQUEST, State.ERROR_DURING_REQUEST)
        .permit(TriggerEvent.UNEXPECTED_CLIENT_MESSAGE_DURING_REQUEST, State.ERROR_DURING_REQUEST)
        .permit(TriggerEvent.CHANNEL_CLOSED_DURING_REQUEST, State.COMPLETING_REQUEST)
        .permit(TriggerEvent.CHANNEL_EXCEPTION_DURING_REQUEST, State.ERROR_DURING_REQUEST)
        .permit(TriggerEvent.SEND_DATA_ERROR, State.ERROR_DURING_REQUEST)
        .permit(TriggerEvent.RESUME, State.READING_DATA)
        .permit(TriggerEvent.CANCELLED, State.CLIENT_CANCEL);
    config.configure(State.EOF)
        .onEntryFrom(mTriggerEventsWithParam.mOutputLengthFulfilled, this::sendEof)
        .permit(TriggerEvent.COMPLETE_REQUEST, State.COMPLETING_REQUEST)
        .permit(TriggerEvent.SEND_DATA_ERROR, State.COMPLETING_REQUEST);
    config.configure(State.CLIENT_CANCEL)
        .onEntryFrom(mTriggerEventsWithParam.mClientCancelled, this::replyCancel)
        .permit(TriggerEvent.COMPLETE_REQUEST, State.COMPLETING_REQUEST)
        .permit(TriggerEvent.SEND_DATA_ERROR, State.COMPLETING_REQUEST);
    config.configure(State.ERROR_OUTSIDE_REQUEST)
        .onEntryFrom(mTriggerEventsWithParam.mPacketReaderCreationError,
            this::replyPacketReaderCreationError)
        .onEntryFrom(mTriggerEventsWithParam.mUnexpectedClientMessage,
            this::replyUnexpectedClientMessage)
        .permit(TriggerEvent.REPLY_MESSAGE_DONE, State.CHANNEL_IDLE)
        .permit(TriggerEvent.REPLY_MESSAGE_ERROR, State.TERMINATED_EXCEPTIONALLY);
    config.configure(State.ERROR_DURING_REQUEST)
        .onEntryFrom(mTriggerEventsWithParam.mInterruptedDuringReq, this::replyError)
        .onEntryFrom(mTriggerEventsWithParam.mUnexpectedClientMessageDuringReq,
            this::replyUnexpectedClientMessageDuringReq)
        .onEntryFrom(mTriggerEventsWithParam.mChannelExceptionDuringReq, this::replyError)
        .onEntryFrom(mTriggerEventsWithParam.mSendDataError, this::replyError)
        .onEntryFrom(mTriggerEventsWithParam.mReadDataError, this::replyError)
        .permit(TriggerEvent.COMPLETE_REQUEST, State.COMPLETING_REQUEST)
        .permit(TriggerEvent.SEND_DATA_ERROR, State.COMPLETING_REQUEST);
    config.configure(State.COMPLETING_REQUEST)
        .onEntryFrom(mTriggerEventsWithParam.mChannelClosedDuringReq, this::completeTask)
        .onEntryFrom(mTriggerEventsWithParam.mCompleteRequest, this::completeTask)
        .onEntryFrom(mTriggerEventsWithParam.mSendDataError, this::completeTaskWithPreviousError)
        .permit(TriggerEvent.REQUEST_COMPLETION_ERROR, State.TERMINATED_EXCEPTIONALLY)
        .permit(TriggerEvent.REQUEST_COMPLETED, State.CHANNEL_IDLE);
    config.configure(State.TERMINATED_EXCEPTIONALLY)
        .substateOf(State.TERMINATED)
        .onEntryFrom(mTriggerEventsWithParam.mReplyMessageError, this::logError)
        .onEntryFrom(mTriggerEventsWithParam.mInterrupted, this::logError)
        .onEntryFrom(mTriggerEventsWithParam.mChannelException, this::logError)
        .onEntryFrom(mTriggerEventsWithParam.mRequestCompletionError, this::logError)
        .onEntry(this::closeChannel);
    config.configure(State.TERMINATED_NORMALLY)
        .substateOf(State.TERMINATED)
        .onEntry(this::onTerminatedNormally);

    mRequestType = requestType;
    mStateMachine = new StateMachine<>(State.CREATED, config);
    mStateMachine.setTrace(new DebugLoggingTracer<>(LOG));
    mStateMachine.fireInitialTransition();
    mChannel = channel;
    mPacketReaderFactory = packetReaderFactory;
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

    Preconditions.checkState(mStateMachine.isInState(State.TERMINATED),
        "execution of state machine has stopped but it is not in a terminated state");
  }

  private void onChannelIdle() {
    while (true) {
      boolean returnNow = takeAndHandleChannelEvent(new ChannelEventVisitor<ReqT>() {
        @Override
        public boolean visit(ReadRequestReceived<ReqT> channelEvent) {
          fireNext(mTriggerEventsWithParam.mRequestReceived, channelEvent.getRequest());
          return true;
        }

        @Override
        public boolean visit(WriteFutureResolved channelEvent) {
          // there is no active writing in progress, just ignore it
          return false;
        }

        @Override
        public boolean visit(ChannelErrorOccurred channelEvent) {
          fireNext(mTriggerEventsWithParam.mChannelException, channelEvent.getCause());
          return true;
        }

        @Override
        public boolean visit(ChannelDisconnected channelEvent) {
          // client actively closed the channel, or the channel is auto closed due to being idle for
          // a long time.
          fireNext(TriggerEvent.CHANNEL_CLOSED);
          return true;
        }

        @Override
        public boolean visit(ClientCanceled channelEvent) {
          // there is no active read request in progress, so this comes from a faulty client
          fireNext(mTriggerEventsWithParam.mUnexpectedClientMessage, channelEvent);
          return true;
        }
      });
      if (returnNow) {
        return;
      }
    }
  }

  private void startNewRequest(ReqT request, Transition<State, TriggerEvent> transition) {
    final PacketReader<ReqT> packetReader;
    try {
      packetReader = mPacketReaderFactory.create(request);
    } catch (Exception e) {
      LOG.error("Failed to create packet reader", e);
      fireNext(mTriggerEventsWithParam.mPacketReaderCreationError, e);
      return;
    }
    RequestContext requestContext =
        RequestContext.createForNewRequest(mRequestType, request, packetReader);
    readData(requestContext, transition);
  }

  private void readData(RequestContext requestContext, Transition<State, TriggerEvent> transition) {
    // casting to int is safe as mRequest.getPacketSize() <= Integer.MAX_VALUE
    final int packetSize = (int) Math.min(
        requestContext.positionEnd() - requestContext.positionRead(),
        (long) requestContext.getRequest().getPacketSize());
    PacketReader<ReqT> packetReader = requestContext.getPacketReader(mRequestType);
    final DataBuffer packet;
    try {
      packet = packetReader.createDataBuffer(
          mChannel, requestContext.positionRead(), packetSize);
    } catch (Exception e) {
      LOG.error("Failed to read data.", e);
      if (!(e instanceof IOException)) {
        IOException ioException = new IOException("Failed to read data from data store", e);
        fireNext(mTriggerEventsWithParam.mReadDataError, requestContext, ioException);
      } else {
        fireNext(mTriggerEventsWithParam.mReadDataError, requestContext, (IOException) e);
      }
      return;
    }
    if (packet.readableBytes() == 0) {
      // The packet can be released when there is nothing more to read
      packet.release();
      // an empty packet means the underlying storage thinks it's EOF
      if (requestContext.positionRead() != requestContext.positionEnd()) {
        // it's possible that client expects more than what's available
        // this is not an error
        LOG.debug("Early EOF when trying to read from "
                + "data source, current position: {}, requested end position: {}, request: {}",
            requestContext.positionRead(), requestContext.positionEnd(),
            requestContext.getRequest());
      }
      fireNext(mTriggerEventsWithParam.mOutputLengthFulfilled, requestContext);
      return;
    }
    // The packet will be released in sendData() where the channel flushes the response
    requestContext.increaseReadProgress(packet.readableBytes());
    fireNext(mTriggerEventsWithParam.mDataAvailable, requestContext, packet);
  }

  private void sendData(RequestContext requestContext, DataBuffer dataBuffer,
      Transition<State, TriggerEvent> transition) {
    final int length = dataBuffer.readableBytes();
    RPCProtoMessage response = RPCProtoMessage.createOkResponse(dataBuffer);
    mChannel.writeAndFlush(response)
        .addListener((ChannelFuture future) -> {
          MultiDimensionalMetricsSystem.DATA_ACCESS.labelValues("read").observe(length);
          if (!future.isSuccess()) {
            LOG.error("Failed to send packet.", future.cause());
            mChannelEventQueue.add(WriteFutureResolved.failure(future.cause()));
            return;
          }
          mChannelEventQueue.put(WriteFutureResolved.success(length));
        });
    if (requestContext.bytesPending()
        >= (long) MAX_PACKETS_IN_FLIGHT * requestContext.getRequest().getPacketSize()) {
      fireNext(mTriggerEventsWithParam.mTooManyPendingPackets, requestContext);
      return;
    }
    // right after the write, take a look at the event queue, and process potential cancel
    // events, as well as write callbacks from previous writes
    // we use BlockingQueue.poll() instead of .take(), as the queue could be empty,
    // and we don't want to block here
    boolean returnNow = pollAndHandleChannelEvent(new ChannelEventVisitor<ReqT>() {
      @Override
      public boolean visit(ReadRequestReceived<ReqT> channelEvent) {
        // the current request is in progress until we send an EOF or the client sends a cancel,
        // whichever happens first. Until then, new requests from client are not allowed
        fireNext(mTriggerEventsWithParam.mUnexpectedClientMessageDuringReq,
            requestContext, channelEvent);
        return true;
      }

      @Override
      public boolean visit(WriteFutureResolved channelEvent) {
        if (channelEvent.isSuccess()) {
          requestContext.increaseWriteProgress(channelEvent.getBytesWritten());
          return false;
        } else {
          fireNext(mTriggerEventsWithParam.mSendDataError, requestContext, channelEvent.getCause());
          return true;
        }
      }

      @Override
      public boolean visit(ChannelErrorOccurred channelEvent) {
        fireNext(mTriggerEventsWithParam.mChannelExceptionDuringReq,
            requestContext, channelEvent.getCause());
        return true;
      }

      @Override
      public boolean visit(ChannelDisconnected channelEvent) {
        fireNext(mTriggerEventsWithParam.mChannelClosedDuringReq, requestContext);
        return true;
      }

      @Override
      public boolean visit(ClientCanceled channelEvent) {
        fireNext(mTriggerEventsWithParam.mClientCancelled, requestContext);
        return true;
      }
    });

    if (returnNow) {
      return;
    }
    if (requestContext.positionRead() < requestContext.positionEnd()) {
      fireNext(mTriggerEventsWithParam.mOutputLengthNotFulfilled, requestContext);
    } else {
      fireNext(mTriggerEventsWithParam.mOutputLengthFulfilled, requestContext);
    }
    return;
  }

  private void sendEof(RequestContext requestContext,
      Transition<State, TriggerEvent> transition) {
    RPCProtoMessage eofMessage = RPCProtoMessage.createOkResponse(null);
    Throwable err = syncReplyMessage(eofMessage);
    if (err != null) {
      fireNext(mTriggerEventsWithParam.mSendDataError, requestContext, err);
    } else {
      fireNext(mTriggerEventsWithParam.mCompleteRequest, requestContext);
    }
  }

  private void replyCancel(RequestContext requestContext,
      Transition<State, TriggerEvent> transition) {
    RPCProtoMessage cancelResponse = RPCProtoMessage.createCancelResponse();
    Throwable err = syncReplyMessage(cancelResponse);
    if (err != null) {
      fireNext(mTriggerEventsWithParam.mSendDataError, requestContext, err);
    } else {
      fireNext(mTriggerEventsWithParam.mCompleteRequest, requestContext);
    }
  }

  /**
   * Writes a message to Netty channel synchronously, blocking the current thread until
   * the write future resolves.
   *
   * @param message message to reply, typically error, cancel, or EOF
   * @return error if failed to send the message, null if success
   */
  @Nullable
  private Throwable syncReplyMessage(RPCProtoMessage message) {
    try {
      ChannelFuture replyMessageFuture = mChannel.writeAndFlush(message).sync();
      if (!replyMessageFuture.isSuccess()) {
        // Note ChannelFuture.sync() will throw exception if the future fails
        // so normally this is unreachable, but just in case
        return replyMessageFuture.cause();
      }
    } catch (Throwable throwable) {
      return throwable;
    }
    return null;
  }

  private void onPaused(RequestContext requestContext, Transition<State, TriggerEvent> transition) {
    LOG.debug("Server entered paused state. Current task progress: "
        + "read pos {}, written pos {}; request start {}, end {}; packetSize {}",
        requestContext.positionRead(), requestContext.positionWritten(),
        requestContext.positionStart(), requestContext.positionEnd(),
        requestContext.getRequest().getPacketSize());
    while (true) {
      boolean retNow = takeAndHandleChannelEvent(requestContext, new ChannelEventVisitor<ReqT>() {
        @Override
        public boolean visit(ReadRequestReceived<ReqT> channelEvent) {
          fireNext(mTriggerEventsWithParam.mUnexpectedClientMessageDuringReq,
              requestContext, channelEvent);
          return true;
        }

        @Override
        public boolean visit(WriteFutureResolved channelEvent) {
          if (channelEvent.isSuccess()) {
            requestContext.increaseWriteProgress(channelEvent.getBytesWritten());
            if (requestContext.bytesPending()
                < ((long) MAX_PACKETS_IN_FLIGHT * requestContext.getRequest().getPacketSize())) {
              fireNext(mTriggerEventsWithParam.mResume, requestContext);
              return true;
            }
            return false;
          } else {
            fireNext(mTriggerEventsWithParam.mSendDataError, requestContext,
                channelEvent.getCause());
            return true;
          }
        }

        @Override
        public boolean visit(ChannelErrorOccurred channelEvent) {
          fireNext(mTriggerEventsWithParam.mChannelExceptionDuringReq,
              requestContext, channelEvent.getCause());
          return true;
        }

        @Override
        public boolean visit(ChannelDisconnected channelEvent) {
          fireNext(mTriggerEventsWithParam.mChannelClosedDuringReq, requestContext);
          return true;
        }

        @Override
        public boolean visit(ClientCanceled channelEvent) {
          fireNext(mTriggerEventsWithParam.mClientCancelled, requestContext);
          return true;
        }
      });

      if (retNow) {
        return;
      }
    }
  }

  private void replyPacketReaderCreationError(Exception cause,
      Transition<State, TriggerEvent> transition) {
    String errorMessage = String.format("Failed to create packet reader: %s", cause);
    RPCProtoMessage errorResponse =
        RPCProtoMessage.createResponse(Status.INTERNAL, errorMessage, null);
    Throwable error = syncReplyMessage(errorResponse);
    if (error != null) {
      fireNext(mTriggerEventsWithParam.mReplyMessageError, error);
    } else {
      fireNext(TriggerEvent.REPLY_MESSAGE_DONE);
    }
  }

  private void replyUnexpectedClientMessage(ChannelEvent unexpectedClientMessage,
      Transition<State, TriggerEvent> transition) {
    LOG.warn("Client sent unexpected request {} when server was in state {} ",
        unexpectedClientMessage, transition.getSource());
    String errorMessage = String.format("Unexpected client message %s when server is in state %s",
        unexpectedClientMessage, transition.getSource());
    Status errorStatus =
        Status.INVALID_ARGUMENT.withDescription("Protocol Violation");
    RPCProtoMessage errorResponse =
        RPCProtoMessage.createResponse(errorStatus, errorMessage, null);
    Throwable error = syncReplyMessage(errorResponse);
    if (error != null) {
      fireNext(mTriggerEventsWithParam.mReplyMessageError, error);
    } else {
      fireNext(TriggerEvent.REPLY_MESSAGE_DONE);
    }
  }

  private void replyUnexpectedClientMessageDuringReq(
      RequestContext requestContext, ChannelEvent unexpectedClientMessage,
      Transition<State, TriggerEvent> transition) {
    LOG.warn("Client sent unexpected request {} when server was in state {} request was {}",
        unexpectedClientMessage, transition.getSource(), requestContext.getRequest());
    MultiDimensionalMetricsSystem.DATA_ACCESS.labelValues("read").observe(0);
    String errorMessage = String.format("Unexpected client message %s when server is in state %s",
        unexpectedClientMessage, transition.getSource());
    Status errorStatus =
        Status.INVALID_ARGUMENT.withDescription("Protocol Violation");
    RPCProtoMessage errorResponse =
        RPCProtoMessage.createResponse(errorStatus, errorMessage, null);
    Throwable error = syncReplyMessage(errorResponse);
    if (error != null) {
      fireNext(mTriggerEventsWithParam.mSendDataError, requestContext, error);
    } else {
      fireNext(mTriggerEventsWithParam.mCompleteRequest, requestContext);
    }
  }

  private void replyError(RequestContext requestContext, Throwable throwable,
      Transition<State, TriggerEvent> transition) {
    LOG.warn("Server error occurred when server was in state {}, triggered by {}. Error: {}",
        transition.getSource(), transition.getTrigger(), throwable.getMessage());
    LOG.debug("Server error occurred when server was in state {}, triggered by {}.",
        transition.getSource(), transition.getTrigger(), throwable);
    MultiDimensionalMetricsSystem.DATA_ACCESS.labelValues("read").observe(0);
    RPCProtoMessage errorResponse =
        RPCProtoMessage.createResponse(AlluxioStatusException.fromThrowable(throwable));
    Throwable error = syncReplyMessage(errorResponse);
    if (error != null) {
      fireNext(mTriggerEventsWithParam.mSendDataError, requestContext, error);
    } else {
      fireNext(mTriggerEventsWithParam.mCompleteRequest, requestContext);
    }
  }

  private void completeTaskWithPreviousError(RequestContext requestContext, Throwable throwable,
      Transition<State, TriggerEvent> transition) {
    PacketReader<ReqT> packetReader = requestContext.getPacketReader(mRequestType);
    try {
      packetReader.close();
      fireNext(TriggerEvent.REQUEST_COMPLETED);
    } catch (Exception e) {
      LOG.error("Failed to complete request", e);
      throwable.addSuppressed(e);
      fireNext(mTriggerEventsWithParam.mRequestCompletionError, throwable);
    }
  }

  private void completeTask(RequestContext requestContext,
      Transition<State, TriggerEvent> transition) {
    PacketReader<ReqT> packetReader = requestContext.getPacketReader(mRequestType);
    try {
      packetReader.close();
      fireNext(TriggerEvent.REQUEST_COMPLETED);
    } catch (Exception e) {
      LOG.error("Failed to complete request", e);
      fireNext(mTriggerEventsWithParam.mRequestCompletionError, e);
    }
  }

  private void logError(Throwable throwable, Transition<State, TriggerEvent> transition) {
    LOG.warn("Encountered an unexpected error {} on channel {} when in state {}, triggered by {}. "
            + "Closing the channel.",
        throwable, mChannel, transition.getSource(), transition.getTrigger());
  }

  private void closeChannel() {
    if (mChannel.isOpen()) {
      CommonUtils.closeChannel(mChannel);
    }
  }

  private void onTerminatedNormally() {
    LOG.debug("Channel {} terminated normally", mChannel);
    closeChannel();
  }

  /**
   * Submits a new read request for processing in the state machine.
   *
   * @param readRequest read request
   */
  public void submitNewRequest(ReqT readRequest) {
    try {
      mChannelEventQueue.put(new ReadRequestReceived<>(readRequest));
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted when enqueuing cancel message from client", e);
    }
  }

  /**
   * Cancel an ongoing read request.
   */
  public void cancel() {
    try {
      mChannelEventQueue.put(new ClientCanceled());
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted when enqueuing cancel message from client", e);
    }
  }

  /**
   * Notify the state machine that a channel exception occur.
   *
   * @param channelException the encountered Error object
   */
  public void notifyChannelException(Throwable channelException) {
    try {
      mChannelEventQueue.put(new ChannelErrorOccurred(channelException));
    } catch (InterruptedException e) {
      RuntimeException toThrow =
          new RuntimeException("Interrupted when enqueuing channel exception", e);
      toThrow.addSuppressed(channelException);
      throw toThrow;
    }
  }

  /**
   * Notifies the state machine that the channel has been closed.
   */
  public void notifyChannelClosed() {
    try {
      mChannelEventQueue.put(new ChannelDisconnected());
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted when enqueuing channel exception", e);
    }
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
   * @see #fireNext(TriggerEvent)
   * @param triggerEvent the next trigger event to fire
   * @param arg0         the argument to be used
   * @param <Arg0T>      the type of the argument to be used
   */
  private <Arg0T> void fireNext(
      TriggerWithParameters1<Arg0T, TriggerEvent> triggerEvent, Arg0T arg0) {
    mNextTriggerEvent.set(() -> mStateMachine.fire(triggerEvent, arg0));
  }

  /**
   * @see #fireNext(TriggerEvent)
   * @param triggerEvent the next trigger event to fire
   * @param arg0         the argument to be used
   * @param <Arg0T>      the type of the first argument to be used
   * @param <Arg1T>      the type of the second argument to be used
   */
  private <Arg0T, Arg1T> void fireNext(
      TriggerWithParameters2<Arg0T, Arg1T, TriggerEvent> triggerEvent, Arg0T arg0, Arg1T arg1) {
    mNextTriggerEvent.set(() -> mStateMachine.fire(triggerEvent, arg0, arg1));
  }
}

