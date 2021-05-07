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

package alluxio.csi.client;

import csi.v1.ControllerGrpc;
import csi.v1.IdentityGrpc;
import csi.v1.NodeGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * A CSI gRPC client, it connects a CSI driver via a given unix domain socket.
 */
public final class CsiGrpcClient implements AutoCloseable {

  private static final Logger LOG =
      LoggerFactory.getLogger(CsiGrpcClient.class);

  private final ManagedChannel mChannel;

  private CsiGrpcClient(ManagedChannel channel) {
    mChannel = channel;
  }

  /**
   * @return builder
   */
  public static GrpcClientBuilder newBuilder() {
    return new GrpcClientBuilder();
  }

  /**
   * The Grpc Client builder.
   */
  public static class GrpcClientBuilder {

    private SocketAddress mSocket;

    /**
     * @param address socket address
     * @return builder
     */
    public GrpcClientBuilder setDomainSocketAddress(SocketAddress address) {
      mSocket = address;
      return this;
    }

    private ManagedChannel getChannel(SocketAddress socketAddress)
        throws IOException {
      DefaultThreadFactory tf = new DefaultThreadFactory(
          "csi-client-", true);
      EpollEventLoopGroup loopGroup = new EpollEventLoopGroup(0, tf);
      if (socketAddress instanceof DomainSocketAddress) {
        ManagedChannel channel = NettyChannelBuilder.forAddress(socketAddress)
            .channelType(EpollDomainSocketChannel.class)
            .eventLoopGroup(loopGroup)
            .usePlaintext()
            .build();
        return channel;
      } else {
        throw new IOException("Currently only unix domain socket is supported");
      }
    }

    /**
     * @return csi client
     * @throws IOException if an I/O error occurs
     */
    public CsiGrpcClient build() throws IOException {
      ManagedChannel socketChannel = getChannel(mSocket);
      return new CsiGrpcClient(socketChannel);
    }
  }

  /**
   * Shutdown the communication channel gracefully,
   * wait for 5 seconds before it is enforced.
   */
  @Override
  public void close() {
    try {
      mChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Failed to gracefully shutdown"
          + " gRPC communication channel in 5 seconds", e);
    }
  }

  /**
   * Creates a blocking stub for CSI identity plugin on the given channel.
   * @return the blocking stub
   */
  public IdentityGrpc.IdentityBlockingStub createIdentityBlockingStub() {
    return IdentityGrpc.newBlockingStub(mChannel);
  }

  /**
   * Creates a blocking stub for CSI controller plugin on the given channel.
   * @return the blocking stub
   */
  public ControllerGrpc.ControllerBlockingStub createControllerBlockingStub() {
    return ControllerGrpc.newBlockingStub(mChannel);
  }

  /**
   * Creates a blocking stub for CSI node plugin on the given channel.
   * @return the blocking stub
   */
  public NodeGrpc.NodeBlockingStub createNodeBlockingStub() {
    return NodeGrpc.newBlockingStub(mChannel);
  }
}
