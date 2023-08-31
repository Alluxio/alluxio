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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.file.ByteBufferTargetBuffer;
import alluxio.file.ReadTargetBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.WorkerNetAddress;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NettyDataReaderStateMachineTest {

  // TODO(Yichuan): Considering final.
  private static final String LOCAL_HOSTNAME = "localhost";
  private static FileSystemContext mFsContext;
  private static NettyDataReaderStateMachine nettyDataReaderStateMachine;
  private static final Protocol.ReadRequest.Builder
      mRequestBuilder = Protocol.ReadRequest.newBuilder();
  private static final WorkerNetAddress mWorkerNetAddress = new WorkerNetAddress()
      .setHost("127.0.0.1").setRpcPort(29998).setDataPort(29999).setWebPort(30000);
  private static final EmbeddedChannel mChannel = new EmbeddedChannel();
  private static final int BYTEBUFFER_CAPACITY = 10;

  private Thread mMachineThread;

  @BeforeClass
  public static void beforeClass() throws IOException {
//    mFsContext = FileSystemContext.create();
    mFsContext = mock(FileSystemContext.class);
    when(mFsContext.acquireNettyChannel(any(WorkerNetAddress.class)))
        .thenReturn(mChannel);
    when(mFsContext.getClusterConf())
        .thenReturn(Configuration.global());
//    doNothing()
//        .when(mFsContext).releaseNettyChannel(any(WorkerNetAddress.class), any(Channel.class));
  }

  @Before
  public void before() {
    ReadTargetBuffer readTargetBuffer = new ByteBufferTargetBuffer(
        ByteBuffer.allocate(BYTEBUFFER_CAPACITY));
    nettyDataReaderStateMachine = new NettyDataReaderStateMachine(
        mFsContext, mWorkerNetAddress, mRequestBuilder, readTargetBuffer);
    mMachineThread = new Thread(() -> {
      nettyDataReaderStateMachine.run();
    });
    mMachineThread.start();
  }

  // TODO(Yichuan): CHANNEL_UNAVAILABLE is not tested.

  @Test
  public void acquiringChannelToChannelActive() throws InterruptedException {
    // Have to sleep for a while to wait for the state machine thread.
    // spin loop? It is just another sleep wait.
    Thread.sleep(3000);
    assertEquals(NettyDataReaderStateMachine.State.CHANNEL_ACTIVE,
        nettyDataReaderStateMachine.getStatus());
  }

  @Test
  public void channelActiveTOReceivedData() throws InterruptedException {
    Thread.sleep(3000);
//    nettyDataReaderStateMachine.fireNext(NettyDataReaderStateMachine.TriggerEvent.INTERRUPTED);
    Thread.sleep(mFsContext.getClusterConf().getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS));
    assertEquals(NettyDataReaderStateMachine.State.CLIENT_CANCEL,
        nettyDataReaderStateMachine.getStatus());
  }

  @Test
  public void channelActiveToInterrupted() throws InterruptedException {
    Thread.sleep(3000);
//    mMachineThread.interrupt();
    nettyDataReaderStateMachine.fireNext(NettyDataReaderStateMachine.TriggerEvent.INTERRUPTED);
    Thread.sleep(30000);
  }
}
