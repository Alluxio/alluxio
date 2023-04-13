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

import alluxio.client.file.dora.netty.NettyDataReaderStateMachine;
import alluxio.exception.status.AlluxioStatusException;

/**
 * Server error response event.
 */
public class ServerErrorResponseEvent implements ResponseEvent {

  private final AlluxioStatusException mAlluxioStatusException;

  /**
   * Server error response event.
   *
   * @param alluxioStatusException the alluxio status exception
   */
  public ServerErrorResponseEvent(AlluxioStatusException alluxioStatusException) {
    mAlluxioStatusException = alluxioStatusException;
  }

  @Override
  public void postProcess(ResponseEventContext responseEventContext) {
    NettyDataReaderStateMachine nettyClientStateMachine =
        responseEventContext.getNettyClientStateMachine();
    nettyClientStateMachine.fireNext(nettyClientStateMachine.getTriggerEventsWithParam()
        .mServerErrorEvent, mAlluxioStatusException);
  }
}
