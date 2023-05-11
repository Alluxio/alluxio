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
 * Response event context is used for recording the following information.
 */
public class ResponseEventContext {

  private final NettyDataReaderStateMachine mNettyClientStateMachine;

  /**
   * Response event context is used for recording the following information.
   * @param nettyClientStateMachine
   */
  public ResponseEventContext(NettyDataReaderStateMachine nettyClientStateMachine) {
    mNettyClientStateMachine = nettyClientStateMachine;
  }

  /**
   * Get the NettyClientStateMachine.
   * @return the NettyClientStateMachine object
   */
  public NettyDataReaderStateMachine getNettyClientStateMachine() {
    return mNettyClientStateMachine;
  }
}
