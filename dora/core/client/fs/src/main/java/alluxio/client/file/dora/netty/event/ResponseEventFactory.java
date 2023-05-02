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

package alluxio.client.file.dora.netty.event;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.network.protocol.databuffer.DataBuffer;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

/**
 * Response Event Factory.
 */
public class ResponseEventFactory {

  private static volatile ResponseEventFactory sResponseEventFactory = null;

  private ResponseEventFactory() {
  }

  /**
   * Get a response event factory.
   *
   * @return response event factory
   */
  public static ResponseEventFactory getResponseEventFactory() {
    if (null == sResponseEventFactory) {
      synchronized (ResponseEventFactory.class) {
        if (null == sResponseEventFactory) {
          sResponseEventFactory = new ResponseEventFactory();
        }
      }
    }
    return sResponseEventFactory;
  }

  /**
   * Create a CancelledResponseEvent.
   *
   * @return CancelledResponseEvent
   */
  public CancelledResponseEvent createCancelResponseEvent() {
    return new CancelledResponseEvent();
  }

  /**
   * Create a DataResponseEvent.
   *
   * @param dataBuffer
   * @return DataResponseEvent
   */
  public DataResponseEvent createDataResponseEvent(DataBuffer dataBuffer) {
    Preconditions.checkState(dataBuffer.getNettyOutput() instanceof ByteBuf,
        "dataBuffer.getNettyOutput is not of type ByteBuf");
    ByteBuf data = (ByteBuf) dataBuffer.getNettyOutput();
    // need to retain this buffer so that it won't get recycled before we are able to
    // process it
    // will be released in reader
    return new DataResponseEvent(data);
  }

  /**
   * Create an EofResponseEvent.
   *
   * @return EofResponseEvent
   */
  public EofResponseEvent createEofResponseEvent() {
    return new EofResponseEvent();
  }

  /**
   * Create an UfsReadHeartBeatResponseEvent.
   *
   * @return UfsReadHeartBeatResponseEvent
   */
  public UfsReadHeartBeatResponseEvent createUfsReadHeartBeatResponseEvent() {
    return new UfsReadHeartBeatResponseEvent();
  }

  /**
   * Create a ServerErrorResponseEvent.
   *
   * @param alluxioStatusException the alluxio status exception
   * @return ServerErrorResponseEvent
   */
  public ServerErrorResponseEvent createServerErrorResponseEvent(
      AlluxioStatusException alluxioStatusException) {
    return new ServerErrorResponseEvent(alluxioStatusException);
  }

  /**
   * Create a ChannelErrorResponseEvent.
   *
   * @param cause the throwable object
   * @return ChannelErrorResponseEvent
   */
  public ChannelErrorResponseEvent createChannelErrorResponseEvent(Throwable cause) {
    return new ChannelErrorResponseEvent(cause);
  }
}
