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

import alluxio.PositionReader;
import alluxio.client.file.FileSystemContext;
import alluxio.file.ReadTargetBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Positioned Netty data reader.
 */
public class NettyDataReader implements PositionReader {
  private final FileSystemContext mContext;
  public final WorkerNetAddress mAddress;
  private final Supplier<Protocol.ReadRequest.Builder> mRequestBuilder;

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
  public int readInternal(long position, ReadTargetBuffer buffer, int length) throws IOException {
    Protocol.ReadRequest.Builder builder = mRequestBuilder.get()
        .setLength(length)
        .setOffset(position)
        .clearCancel();
    NettyDataReaderStateMachine clientStateMachine =
        new NettyDataReaderStateMachine(mContext, mAddress, builder, buffer);
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
}
