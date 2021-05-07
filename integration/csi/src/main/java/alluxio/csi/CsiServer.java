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

package alluxio.csi;

import alluxio.ProjectConstants;
import alluxio.RuntimeConstants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CLI entrypoint of the CSI service daemon.
 */
public class CsiServer {

  private static final Logger LOG = LoggerFactory.getLogger(
      CsiServer.class);

  private void run(AlluxioConfiguration conf) throws Exception {
    EpollEventLoopGroup group = new EpollEventLoopGroup();

    Server server =
        NettyServerBuilder
            .forAddress(new DomainSocketAddress(conf.get(PropertyKey.CSI_DOMAIN_SOCKET_ADDRESS)))
            .channelType(EpollServerDomainSocketChannel.class)
            .workerEventLoopGroup(group)
            .bossEventLoopGroup(group)
            .addService(new IdentityService())
            .addService(new ControllerService(conf.getBytes(PropertyKey.CSI_DEFAULT_VOLUME_SIZE)))
            .addService(new NodeService(conf))
            .build();

    server.start();
    server.awaitTermination();
  }

  /**
   * @param args arguments to run the command line
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Alluxio version: {}-{}", RuntimeConstants.VERSION, ProjectConstants.REVISION);
    AlluxioConfiguration conf = InstancedConfiguration.defaults();
    new CsiServer().run(conf);
  }
}
