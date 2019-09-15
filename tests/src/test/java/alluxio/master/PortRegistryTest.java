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

package alluxio.master;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.master.PortRegistry.Registry;
import alluxio.network.PortUtils;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

/**
 * Unit tests for {@link PortRegistry}.
 */
public final class PortRegistryTest {
  private Registry mRegistry = new Registry();

  @Test
  public void lockOnce() {
    int port = -1;
    boolean locked = false;
    for (int i = 0; i < 100; i++) {
      port = PortUtils.getFreePort();
      if (mRegistry.lockPort(port)) {
        locked = true;
        break;
      }
    }
    assertTrue(locked);
    for (int i = 0; i < 100; i++) {
      assertFalse(mRegistry.lockPort(port));
    }
  }

  @Test
  public void lockMany() {
    int numPorts = 20;
    Set<Integer> ports = new HashSet<>();
    for (int i = 0; i < numPorts; i++) {
      ports.add(mRegistry.reservePort());
    }
    assertEquals(numPorts, ports.size());
  }

  @Test
  public void lockAndRelease() {
    int port = PortUtils.getFreePort();
    int successes = 0;
    for (int i = 0; i < 10; i++) {
      if (mRegistry.lockPort(port)) {
        successes++;
        mRegistry.release(port);
      }
    }
    // Other processes could interfere and steal the lock occasionally, so we only check > 50.
    assertThat(successes, greaterThan(5));
  }

  @Test
  public void releaseDeletesFile() {
    int successes = 0;
    for (int i = 0; i < 5; i++) {
      int port = mRegistry.reservePort();
      File portFile = mRegistry.portFile(port);
      assertTrue(portFile.exists());
      mRegistry.release(port);
      if (!portFile.exists()) {
        successes++;
      }
    }
    assertThat(successes, greaterThan(2));
  }

  @Test
  public void reserveSamePort() throws Exception {
    int port = 0;
    for (int i = 0; i < 1000; i++) {
      try (ServerSocket socket = new ServerSocket(port)) {
        socket.setReuseAddress(true);
        port = socket.getLocalPort();
      }
    }
  }

  @Test
  public void reservation() throws Exception {
    for (int i = 0; i < 20000; i++) {
      int port = mRegistry.reservePort();

      Server server = NettyServerBuilder.forAddress(new InetSocketAddress(port))
          .channelType(NioServerSocketChannel.class).build();
      try {
        server.start();
      } catch (Exception e) {
        fail("failed to serve on port: " + port);
      }

      assertEquals(port, server.getPort());
      assertFalse(server.isShutdown());
      assertFalse(server.isTerminated());
      server.shutdownNow();
      server.awaitTermination();
      assertTrue(server.isShutdown());
      assertTrue(server.isTerminated());

      mRegistry.release(port);
    }
  }

  @Ignore("This takes longer and is only for performance evaluations")
  @Test
  public void reservePerformance() {
    int count = 200000;
    long startMs = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      int port = mRegistry.reservePort();
      mRegistry.release(port);
    }
    long endMs = System.currentTimeMillis();
    System.out
        .println(String.format("count: %d duration: %.1f s", count, (endMs - startMs) / 1000.0));
  }
}
