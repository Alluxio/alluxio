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

package alluxio.worker.netty;

import static org.junit.Assert.assertTrue;

import alluxio.client.netty.ClientHandler;
import alluxio.client.netty.NettyClient;
import alluxio.client.netty.SingleResponseListener;
import alluxio.network.protocol.RPCBlockWriteRequest;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataByteArrayChannel;
import alluxio.util.CommonUtils;
import alluxio.worker.AlluxioWorker;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.file.FileSystemWorker;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link NettyDataServer}. The entire alluxio.worker.netty package is treated as one
 * unit for the purposes of these tests; all classes except for NettyDataServer are package-private.
 */
public final class NettyDataServerTest {
  private NettyDataServer mNettyDataServer;
  private BlockWorker mBlockWorker;
  private FileSystemWorker mFileSystemWorker;

  @Before
  public void before() {
    mBlockWorker = Mockito.mock(BlockWorker.class);
    mFileSystemWorker = Mockito.mock(FileSystemWorker.class);
    AlluxioWorker alluxioWorker = Mockito.mock(AlluxioWorker.class);
    Mockito.when(alluxioWorker.getBlockWorker()).thenReturn(mBlockWorker);
    Mockito.when(alluxioWorker.getFileSystemWorker()).thenReturn(mFileSystemWorker);

    mNettyDataServer = new NettyDataServer(new InetSocketAddress(0), alluxioWorker);
  }

  @After
  public void after() throws Exception {
    mNettyDataServer.close();
  }

  @Test
  public void closeTest() throws Exception {
    mNettyDataServer.close();
  }

  @Test
  public void portTest() {
    assertTrue(mNettyDataServer.getPort() > 0);
  }

  @Test
  public void writeBlockTest() throws Exception {
    InetSocketAddress address = new InetSocketAddress(mNettyDataServer.getBindHost(), mNettyDataServer.getPort());
    ClientHandler handler = new ClientHandler();
    Bootstrap clientBootstrap = NettyClient.createClientBootstrap(handler);
    ChannelFuture f = clientBootstrap.connect(address).sync();
    Channel channel = f.channel();
    SingleResponseListener listener = new SingleResponseListener();
    handler.addListener(listener);
    channel.writeAndFlush(new RPCBlockWriteRequest(0, 1, 2, 3,
        new DataByteArrayChannel(new byte[] {1, 2, 3}, 1, 1)));
    RPCResponse response = listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    channel.close().sync();
    CommonUtils.sleepMs(100000000);
  }
}
