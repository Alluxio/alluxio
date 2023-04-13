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

/**
 * UFS Read Heartbeat Response Event.
 */
public class UfsReadHeartBeatResponseEvent implements ResponseEvent {

  @Override
  public void postProcess(ResponseEventContext responseEventContext) {
    NettyDataReaderStateMachine nettyClientStateMachine =
        responseEventContext.getNettyClientStateMachine();
    nettyClientStateMachine.fireNext(NettyDataReaderStateMachine.TriggerEvent.HEART_BEAT);
  }
}
