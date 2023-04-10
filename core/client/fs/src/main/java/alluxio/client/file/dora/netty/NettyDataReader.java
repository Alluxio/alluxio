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

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.dora.DoraDataReader;
import alluxio.client.file.dora.netty.PartialReadException.CauseType;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.function.Supplier;

/**
 * Positioned Netty data reader.
 */
public class NettyDataReader implements DoraDataReader {
  private static final Logger LOG = LoggerFactory.getLogger(NettyDataReader.class);
  private final FileSystemContext mContext;
  private final WorkerNetAddress mAddress;
  private final Supplier<Protocol.ReadRequest.Builder> mRequestBuilder;
  private boolean mClosed = false;

  /**
   * Constructor.
   *
   * @param context
   * @param address
   * @param requestBuilder
   */
  public NettyDataReader(FileSystemContext context, WorkerNetAddress address,
      Protocol.ReadRequest.Builder requestBuilder) {
    mContext = context;
    mAddress = address;
    // clone the builder so that the initial values does not get overridden
    mRequestBuilder = requestBuilder::clone;
  }

  @Override
  public int read(long offset, WritableByteChannel outChannel, int length)
      throws PartialReadException {
    Preconditions.checkState(!mClosed, "Reader is closed");
    Protocol.ReadRequest.Builder builder = mRequestBuilder.get()
        .setLength(length)
        .setOffset(offset)
        .clearCancel();
    NettyDataReaderStateMachine clientStateMachine =
        new NettyDataReaderStateMachine(mContext, mAddress, builder, outChannel);
    clientStateMachine.run();
    int bytesRead = clientStateMachine.getBytesRead();
    PartialReadException exception = clientStateMachine.getException();
    if (exception != null) {
      throw exception;
    } else {
      if (bytesRead == 0) {
        return -1;
      }
      return bytesRead;
    }
  }

  @Override
  public void readFully(long offset, WritableByteChannel outChannel, int length)
      throws PartialReadException {
    int totalBytesRead = 0;
    while (totalBytesRead < length) {
      int bytesToRead = length - totalBytesRead;
      try {
        // todo(bowen): adjust timeout on each retry to account for the total expected timeout
        int bytesRead = read(offset + totalBytesRead, outChannel, bytesToRead);
        if (bytesRead < 0) { // eof
          break;
        }
        offset += bytesRead;
        totalBytesRead += bytesRead;
      } catch (PartialReadException e) {
        int bytesRead = e.getBytesRead();
        offset += bytesRead;
        totalBytesRead += bytesRead;
        if (bytesRead == 0) {
          // the last attempt did not make any progress, giving up
          LOG.warn("Giving up read due to no progress can be made: {} ({}), "
                  + "{} bytes requested, {} bytes read so far",
              e.getCauseType(), e.getCause().getMessage(), length, totalBytesRead);
          throw new PartialReadException(length, totalBytesRead, e.getCauseType(), e.getCause());
        }
        // decide if the error is retryable
        switch (e.getCauseType()) {
          // error cases that cannot be retried
          case SERVER_ERROR:
          case OUTPUT:
            LOG.warn("Giving up read due to unretryable error: {} ({}), "
                    + "{} bytes requested, {} bytes read so far",
                e.getCauseType(), e.getCause().getMessage(), length, totalBytesRead);
            throw new PartialReadException(length, totalBytesRead, e.getCauseType(), e.getCause());
          default:
            LOG.debug("Retrying read on exception {}, current progress: {} read / {} requested",
                e.getCauseType(), totalBytesRead, length, e.getCause());
        }
      }
    }
    if (totalBytesRead < length) {
      throw new PartialReadException(length, totalBytesRead, CauseType.EARLY_EOF,
          new EOFException(String.format("Unexpected EOF: %d bytes wanted, "
              + "%d bytes available", length, totalBytesRead)));
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
  }
}
