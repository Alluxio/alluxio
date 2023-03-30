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

package alluxio.client.file.dora.event;

import alluxio.client.file.dora.PartialReadException;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Data response event.
 */
public class DataResponseEvent implements ResponseEvent {

  private final ByteBuf mByteBuf;

  /**
   * Data response event.
   *
   * @param data
   */
  public DataResponseEvent(ByteBuf data) {
    mByteBuf = data;
  }

  @Override
  public void postProcess(ResponseEventContext responseEventContext) throws PartialReadException {
    // do nothing
    int readableBytes = mByteBuf.readableBytes();
    int sliceEnd = Math.min(readableBytes,
        responseEventContext.getLengthToRead() - responseEventContext.getBytesRead());
    // todo(bowen): handle case where ByteBuf does not support getting a bytebuffer
    ByteBuffer toWrite = mByteBuf.nioBuffer(0, sliceEnd);
    try {
      int bytesRead = responseEventContext.getRelatedOutChannel().write(toWrite);
      responseEventContext.increaseBytesRead(bytesRead);
    } catch (IOException ioe) {
      throw new PartialReadException(responseEventContext.getLengthToRead(),
          responseEventContext.getBytesRead(), PartialReadException.CauseType.OUTPUT, ioe);
    } finally {
      // previously retained in packet read handler
      mByteBuf.release();
    }
  }
}
