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

package alluxio.grpc;

import static alluxio.grpc.GrpcSerializationUtils.addBuffersToStream;

import alluxio.collections.ConcurrentIdentityHashMap;
import alluxio.network.protocol.databuffer.DataBuffer;

import io.grpc.Drainable;
import io.grpc.MethodDescriptor;
import io.grpc.internal.CompositeReadableBuffer;
import io.grpc.internal.ReadableBuffer;
import io.grpc.internal.ReadableBuffers;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * Marshaller for data messages.
 *
 * @param <T> type of the message
 */
public abstract class DataMessageMarshaller<T> implements MethodDescriptor.Marshaller<T>,
    BufferRepository<T, DataBuffer> {
  private final MethodDescriptor.Marshaller<T> mOriginalMarshaller;
  private final Map<T, DataBuffer> mBufferMap = new ConcurrentIdentityHashMap<>();
  private volatile boolean mClosed = false;

  /**
   * Creates a data marshaller.
   *
   * @param originalMarshaller the original marshaller for the message
   */
  public DataMessageMarshaller(MethodDescriptor.Marshaller<T> originalMarshaller) {
    mOriginalMarshaller = originalMarshaller;
  }

  @Override
  public InputStream stream(T message) {
    return new DataBufferInputStream(message);
  }

  @Override
  public T parse(InputStream message) {
    ReadableBuffer rawBuffer = GrpcSerializationUtils.getBufferFromStream(message);
    try {
      if (rawBuffer != null) {
        CompositeReadableBuffer readableBuffer = new CompositeReadableBuffer();
        readableBuffer.addBuffer(rawBuffer);
        return deserialize(readableBuffer);
      } else {
        // falls back to buffer copy
        byte[] byteBuffer = new byte[message.available()];
        message.read(byteBuffer);
        return deserialize(ReadableBuffers.wrap(byteBuffer));
      }
    } catch (Throwable t) {
      if (rawBuffer != null) {
        rawBuffer.close();
      }
      throw new RuntimeException(t);
    }
  }

  @Override
  public void close() {
    mClosed = true;
    for (DataBuffer buffer : mBufferMap.values()) {
      buffer.release();
    }
  }

  @Override
  public void offerBuffer(DataBuffer buffer, T message) {
    if (mClosed) {
      // this marshaller is already closed, so release any resources and do not update the map
      buffer.release();
      return;
    }
    mBufferMap.put(message, buffer);
  }

  @Override
  public DataBuffer pollBuffer(T message) {
    return mBufferMap.remove(message);
  }

  /**
   * Combines the data buffer into the message.
   *
   * @param message the message to be combined
   * @return the message with the combined buffer
   */
  public abstract T combineData(DataMessage<T, DataBuffer> message);

  /**
   * Serialize the message to buffers.
   * @param message the message to be serialized
   * @return an array of {@link ByteBuf}s containing the serialized message
   * @throws IOException if the marshaller fails to serialize the message
   */
  protected abstract ByteBuf[] serialize(T message) throws IOException;

  /**
   * Deserialize data buffer to the message.
   *
   * @param buffer the buffer that contains the message data
   * @return the deserialized message
   * @throws IOException if the marshaller fails to deserialize the data
   */
  protected abstract T deserialize(ReadableBuffer buffer) throws IOException;

  /**
   * A {@link InputStream} for writing a message into a gRPC output stream. It will attempt to
   * insert raw data buffer directly to the target stream if possible, or fallback to buffer copy if
   * the insertion fails.
   */
  private class DataBufferInputStream extends InputStream implements Drainable {
    private final InputStream mStream;
    private final T mMessage;

    DataBufferInputStream(T message) {
      mMessage = message;
      mStream = mOriginalMarshaller.stream(message);
    }

    @Override
    public int read() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(OutputStream target) throws IOException {
      int bytesWritten = 0;
      ByteBuf[] buffers = serialize(mMessage);
      for (ByteBuf buffer : buffers) {
        bytesWritten += buffer.readableBytes();
      }
      if (!addBuffersToStream(buffers, target)) {
        // falls back to buffer copy
        for (ByteBuf buffer : buffers) {
          buffer.readBytes(target, buffer.readableBytes());
        }
      }
      return bytesWritten;
    }

    @Override
    public void close() throws IOException {
      mStream.close();
    }
  }
}
