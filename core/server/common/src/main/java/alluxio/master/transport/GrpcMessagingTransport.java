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

package alluxio.master.transport;

import alluxio.conf.AlluxioConfiguration;
import alluxio.security.user.UserState;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Transport implementation based on Alluxio gRPC messaging.
 */
public class GrpcMessagingTransport {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcMessagingTransport.class);

  /** Alluxio configuration for clients. */
  private final AlluxioConfiguration mClientConf;
  /** Alluxio configuration for servers. */
  private final AlluxioConfiguration mServerConf;
  /** User for clients. */
  private final UserState mClientUser;
  /** User for servers. */
  private final UserState mServerUser;
  /** Used to distinguish between multiple users of the transport. */
  private final String mClientType;

  /** List of created clients. */
  private final List<GrpcMessagingClient> mClients;
  /** List of created servers. */
  private final List<GrpcMessagingServer> mServers;

  /** External proxy configuration for servers. */
  private GrpcMessagingProxy mServerProxy = new GrpcMessagingProxy();

  /** Executor that is used by clients/servers for building connections. */
  private final ExecutorService mExecutor;

  /** Whether the transport is closed. */
  private boolean mClosed;

  /**
   * Creates {@link GrpcMessagingTransport} based on Alluxio gRPC messaging.
   *
   * @param conf Alluxio configuration
   * @param user Alluxio user
   * @param clientType Transport client type
   */
  public GrpcMessagingTransport(AlluxioConfiguration conf, UserState user, String clientType) {
    this(conf, conf, user, user, clientType);
  }

  /**
   * Creates {@link GrpcMessagingTransport} based on Alluxio gRPC messaging.
   *
   * @param clientConf Alluxio configuration for clients
   * @param serverConf Alluxio configuration for servers
   * @param clientUser User for clients
   * @param serverUser User for servers
   * @param clientType Transport client type
   */
  public GrpcMessagingTransport(AlluxioConfiguration clientConf, AlluxioConfiguration serverConf,
      UserState clientUser, UserState serverUser, String clientType) {
    mClientConf = clientConf;
    mServerConf = serverConf;
    mClientUser = clientUser;
    mServerUser = serverUser;
    mClientType = clientType;

    mClients = new LinkedList<>();
    mServers = new LinkedList<>();
    mExecutor = Executors
        .newCachedThreadPool(ThreadFactoryUtils.build("grpc-messaging-transport-worker-%d", true));
  }

  /**
   * Sets external proxy configuration for servers.
   *
   * @param proxy external proxy configuration
   * @return the updated transport instance
   */
  public synchronized GrpcMessagingTransport withServerProxy(GrpcMessagingProxy proxy) {
    Preconditions.checkNotNull(proxy, "Server proxy reference cannot be null.");
    mServerProxy = proxy;
    return this;
  }

  /**
   * Creates a new Grpc messaging client.
   *
   * @return the created client
   */
  public synchronized GrpcMessagingClient client() {
    if (mClosed) {
      throw new RuntimeException("Messaging transport closed");
    }
    GrpcMessagingClient client =
        new GrpcMessagingClient(mClientConf, mClientUser, mExecutor, mClientType);
    mClients.add(client);
    return client;
  }

  /**
   * Creates a new Grpc messaging server.
   *
   * @return the created server
   */
  public synchronized GrpcMessagingServer server() {
    if (mClosed) {
      throw new RuntimeException("Messaging transport closed");
    }
    GrpcMessagingServer server =
        new GrpcMessagingServer(mServerConf, mServerUser, mExecutor, mServerProxy);
    mServers.add(server);
    return server;
  }

  /**
   * Closes the opened clients and servers.
   */
  public synchronized void close() {
    if (!mClosed) {
      mClosed = true;

      // Close created clients.
      List<CompletableFuture<Void>> clientCloseFutures = new ArrayList<>(mClients.size());
      for (GrpcMessagingClient client : mClients) {
        clientCloseFutures.add(client.close());
      }
      mClients.clear();
      try {
        CompletableFuture.allOf(clientCloseFutures.toArray(new CompletableFuture[0])).get();
      } catch (Exception e) {
        LOG.warn("Failed to close messaging transport clients.", e);
      }

      // Close created servers.
      List<CompletableFuture<Void>> serverCloseFutures = new ArrayList<>(mServers.size());
      for (GrpcMessagingServer server : mServers) {
        serverCloseFutures.add(server.close());
      }
      mServers.clear();
      try {
        CompletableFuture.allOf(serverCloseFutures.toArray(new CompletableFuture[0])).get();
      } catch (Exception e) {
        LOG.warn("Failed to close messaging transport servers.", e);
      }

      // Shut down transport executor.
      mExecutor.shutdownNow();
    }
  }
}
