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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.Chunk;
import alluxio.grpc.ReadResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.resource.LockResource;

import com.google.protobuf.ByteString;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link ReadRequest}s.
 *
 * Protocol: Check {@link alluxio.client.block.stream.GrpcDataReader} for additional information.
 * 1. Once a read request is received, the handler creates a {@link PacketReader} which reads
 *    packets from the block worker and pushes them to the buffer.
 * 2. The {@link PacketReader} pauses if there are too many packets in flight, and resumes if there
 *    is room available.
 * 3. The channel is closed if there is any exception during the packet read/write.
 *
 * Threading model:
 * Only two threads are involved at a given point of time: gRPC event thread, packet reader thread.
 * 1. The gRPC event thread accepts the read request, handles write callbacks. If any exception
 *    occurs (e.g. failed to read from stream or respond to stream) or the read request is cancelled
 *    by the client, the gRPC event thread notifies the packet reader thread.
 * 2. The packet reader thread keeps reading from the file and writes to buffer. Before reading a
 *    new packet, it checks whether there are notifications (e.g. cancel, error), if
 *    there is, handle them properly. See more information about the notifications in the javadoc
 *    of {@link ReadRequestContext#mCancel#mEof#mError}.
 *
 * @param <T> type of read request
 */
@NotThreadSafe
abstract class AbstractReadHandler<T extends ReadRequestContext<?>> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractReadHandler.class);
  private static final long PACKET_SIZE =
      Configuration.getBytes(PropertyKey.USER_LOCAL_READER_PACKET_SIZE_BYTES);

  /** The executor to run {@link PacketReader}. */
  private final ExecutorService mPacketReaderExecutor;

  private final ReentrantLock mLock = new ReentrantLock();

  /**
   * This is only created in the netty I/O thread when a read request is received, reset when
   * another request is received.
   * Using "volatile" because we want any value change of this variable to be
   * visible across both netty and I/O threads, meanwhile no atomicity of operation is assumed;
   */
  private volatile T mContext;
  private StreamObserver<ReadResponse> mResponseObserver;

  /**
   * Creates an instance of {@link AbstractReadHandler}.
   *
   * @param executorService the executor service to run {@link PacketReader}s
   */
  AbstractReadHandler(ExecutorService executorService) {
    mPacketReaderExecutor = executorService;
  }

  public void readBlock(alluxio.grpc.ReadRequest request,  StreamObserver<ReadResponse> response)
      throws Exception {
    mResponseObserver = response;
    // Expected state: context equals null as this handler is new for request.
    // Otherwise, notify the client an illegal state. Note that, we reset the context before
    // validation msg as validation may require to update error in context.
    try (LockResource lr = new LockResource(mLock)) {
      Preconditions.checkState(mContext == null || !mContext.isPacketReaderActive());
      mContext = createRequestContext(request);
    }
    validateReadRequest(request);
    try (LockResource lr = new LockResource(mLock)) {
      mContext.setPosToQueue(mContext.getRequest().getStart());
      mContext.setPosToWrite(mContext.getRequest().getStart());
      mPacketReaderExecutor.submit(createPacketReader(mContext, response));
      mContext.setPacketReaderActive(true);
    }
  }

  public void onError(Throwable cause) {
    LOG.error("Exception caught in AbstractReadHandler:", cause);
    setError(new Error(AlluxioStatusException.fromThrowable(cause), false));
  }

  /**
   * Validates a read request.
   *
   * @param request the block read request
   * @throws InvalidArgumentException if the request is invalid
   */
  private void validateReadRequest(alluxio.grpc.ReadRequest request)
      throws InvalidArgumentException {
    if (request.getBlockId() < 0) {
      throw new InvalidArgumentException(
          String.format("Invalid blockId (%d) in read request.", request.getBlockId()));
    }
    if (request.getOffset() < 0 || request.getLength() <= 0) {
      throw new InvalidArgumentException(
          String.format("Invalid read bounds in read request %s.", request.toString()));
    }
  }

  /**
   * @param error the error
   */
  private void setError(Error error) {
    Preconditions.checkNotNull(error, "error");
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null || mContext.getError() != null || mContext.isDoneUnsafe()) {
        // Note, we may reach here via channelUnregistered due to network errors bubbling up before
        // mContext is initialized, or channel garbage collection after the request is finished.
        return;
      }
      mContext.setError(error);
      if (!mContext.isPacketReaderActive()) {
        mContext.setPacketReaderActive(true);
        mPacketReaderExecutor.submit(createPacketReader(mContext, mResponseObserver));
      }
    }
  }

  private void setEof() {
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null || mContext.getError() != null || mContext.isCancel()
          || mContext.isEof()) {
        return;
      }
      mContext.setEof(true);
      if (!mContext.isPacketReaderActive()) {
        mContext.setPacketReaderActive(true);
        mPacketReaderExecutor.submit(createPacketReader(mContext, mResponseObserver));
      }
    }
  }

  private void setCancel() {
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null || mContext.getError() != null || mContext.isEof()
          || mContext.isCancel()) {
        return;
      }
      mContext.setCancel(true);
      if (!mContext.isPacketReaderActive()) {
        mContext.setPacketReaderActive(true);
        mPacketReaderExecutor.submit(createPacketReader(mContext, mResponseObserver));
      }
    }
  }

  /**
   * @param request the block read request
   * @return an instance of read request based on the request read from channel
   */
  protected abstract T createRequestContext(alluxio.grpc.ReadRequest request);

  /**
   * Creates a read reader.
   *
   * @param context read request context
   * @param channel channel
   * @return the packet reader for this handler
   */
  protected abstract PacketReader createPacketReader(T context,
      StreamObserver<ReadResponse> channel);

  public void onCancel() {
    setCancel();
  }

  public void onReady() {
    try (LockResource lr = new LockResource(mLock)) {
      if (shouldRestartPacketReader()) {
        mPacketReaderExecutor.submit(createPacketReader(mContext, mResponseObserver));
        mContext.setPacketReaderActive(true);
      }
    }
  }

  /**
   * @return true if we should restart the packet reader
   */
  @GuardedBy("mLock")
  private boolean shouldRestartPacketReader() {
    return !mContext.isPacketReaderActive()
        && mContext.getPosToQueue() < mContext.getRequest().getEnd()
        && mContext.getError() == null && !mContext.isCancel() && !mContext.isEof();
  }

  /**
   * @param bytesRead bytes read
   */
  private void incrementMetrics(long bytesRead) {
    Counter counter = mContext.getCounter();
    Meter meter = mContext.getMeter();
    Preconditions.checkState(counter != null);
    counter.inc(bytesRead);
    meter.mark(bytesRead);
  }

  /**
   * A runnable that reads packets and writes them to the channel.
   */
  protected abstract class PacketReader implements Runnable {
    private final ServerCallStreamObserver<ReadResponse> mResponse;
    private final T mContext;
    private final ReadRequest mRequest;

    /**
     * Creates an instance of the {@link PacketReader}.
     *
     * @param context context of the request to complete
     * @param response the response
     */
    PacketReader(T context, StreamObserver<ReadResponse> response) {
      mContext = context;
      mRequest = context.getRequest();
      mResponse = (ServerCallStreamObserver<ReadResponse>) response;
    }

    @Override
    public void run() {
      try {
        runInternal();
      } catch (Throwable e) {
        LOG.error("Failed to run PacketReader.", e);
        throw new RuntimeException(e);
      }
    }

    private void runInternal() {
      boolean eof;  // End of file. Everything requested has been read.
      boolean cancel;
      Error error;  // error occurred, abort requested.
      while (true) {
        final long start;
        final int packetSize;
        try (LockResource lr = new LockResource(mLock)) {
          start = mContext.getPosToQueue();
          eof = mContext.isEof();
          cancel = mContext.isCancel();
          error = mContext.getError();

          if (eof || cancel || error != null || !mResponse.isReady()) {
            mContext.setPacketReaderActive(false);
            break;
          }
          packetSize = (int) Math
              .min(mRequest.getEnd() - mContext.getPosToQueue(), PACKET_SIZE);

          // packetSize should always be > 0 here when reaches here.
          Preconditions.checkState(packetSize > 0);
        }

        DataBuffer packet;
        try {
          packet = getDataBuffer(mContext, mResponse, start, packetSize);
        } catch (Exception e) {
          LOG.error("Failed to read data.", e);
          setError(new Error(AlluxioStatusException.fromThrowable(e), true));
          continue;
        }
        if (packet != null) {
          try (LockResource lr = new LockResource(mLock)) {
            mContext.setPosToQueue(mContext.getPosToQueue() + packet.getLength());
          }
        }
        if (packet == null || packet.getLength() < packetSize || start + packetSize == mRequest
            .getEnd()) {
          // This can happen if the requested read length is greater than the actual length of the
          // block or file starting from the given offset.
          setEof();
        }

        if (packet != null) {
          ReadResponse response = ReadResponse.newBuilder().setChunk(Chunk.newBuilder()
              .setData(ByteString.copyFrom(packet.getReadOnlyByteBuffer())).build())
              .build();
          mResponse.onNext(response);
          incrementMetrics(packet.getLength());
        }
      }

      if (error != null) {
        try {
          // mRequest is null if an exception is thrown when initializing mRequest.
          if (mRequest != null) {
            completeRequest(mContext);
          }
        } catch (Exception e) {
          LOG.error("Failed to close the request.", e);
        }
        replyError(error);
      } else if (eof || cancel) {
        try {
          completeRequest(mContext);
        } catch (IOException e) {
          setError(new Error(AlluxioStatusException.fromIOException(e), true));
        } catch (Exception e) {
          setError(new Error(AlluxioStatusException.fromThrowable(e), true));
        }
        if (eof) {
          replyEof();
        } else {
          replyCancel();
        }
      }
    }

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
     * @param response the gRPC response observer
     * @param len The length, in bytes, of the data to read from the block
     * @return a {@link DataBuffer} representing the data
     */
    protected abstract DataBuffer getDataBuffer(T context, StreamObserver<ReadResponse> response,
        long offset, int len) throws Exception;

    /**
     * Writes an error read response to the channel and closes the channel after that.
     */
    private void replyError(Error error) {
      mResponse.onError(error.getCause());
    }

    /**
     * Writes a success response.
     */
    private void replyEof() {
      Preconditions.checkState(!mContext.isDoneUnsafe());
      mContext.setDoneUnsafe(true);
      mResponse.onCompleted();
    }

    /**
     * Writes a cancel response.
     */
    private void replyCancel() {
      Preconditions.checkState(!mContext.isDoneUnsafe());
      mContext.setDoneUnsafe(true);
      mResponse.onError(null);
    }
  }
}
