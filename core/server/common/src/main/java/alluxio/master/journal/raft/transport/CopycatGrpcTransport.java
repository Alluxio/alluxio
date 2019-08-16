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

package alluxio.master.journal.raft.transport;

import alluxio.conf.AlluxioConfiguration;
import alluxio.security.user.UserState;
import alluxio.util.ThreadFactoryUtils;

import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Copycat {@link Transport} implementation that uses Alluxio gRPC.
 */
public class CopycatGrpcTransport implements Transport {
  private static final Logger LOG = LoggerFactory.getLogger(CopycatGrpcTransport.class);

  /** Alluxio configuration for clients. */
  private final AlluxioConfiguration mClientConf;
  /** Alluxio configuration for servers. */
  private final AlluxioConfiguration mServerConf;
  /** User for clients. */
  private final UserState mClientUser;
  /** User for servers. */
  private final UserState mServerUser;

  /** List of created clients. */
  private final List<CopycatGrpcClient> mClients;
  /** List of created servers. */
  private final List<CopycatGrpcServer> mServers;

  /** Executor that is used by clients/servers for building connections. */
  private final ExecutorService mExecutor;

  /** Whether the transport is closed. */
  private boolean mClosed;

  /**
   * Creates {@link Transport} for CopyCat that uses Alluxio gRPC.
   *
   * @param conf Alluxio configuration
   * @param user Alluxio user
   */
  public CopycatGrpcTransport(AlluxioConfiguration conf, UserState user) {
    this(conf, conf, user, user);
  }

  /**
   * Creates {@link Transport} for CopyCat that uses Alluxio gRPC.
   *
   * @param clientConf Alluxio configuration for clients
   * @param serverConf Alluxio configuration for servers
   * @param clientUser User for clients
   * @param serverUser User for servers
   */
  public CopycatGrpcTransport(AlluxioConfiguration clientConf, AlluxioConfiguration serverConf,
      UserState clientUser, UserState serverUser) {
    mClientConf = clientConf;
    mServerConf = serverConf;
    mClientUser = clientUser;
    mServerUser = serverUser;

    mClients = new LinkedList<>();
    mServers = new LinkedList<>();
    mExecutor = Executors
        .newCachedThreadPool(ThreadFactoryUtils.build("copycat-transport-worker-%d", true));
  }

  @Override
  public synchronized Client client() {
    if (mClosed) {
      throw new RuntimeException("Transport closed");
    }
    CopycatGrpcClient client = new CopycatGrpcClient(mClientConf, mClientUser, mExecutor);
    mClients.add(client);
    return client;
  }

  @Override
  public synchronized Server server() {
    if (mClosed) {
      throw new RuntimeException("Transport closed");
    }
    CopycatGrpcServer server = new CopycatGrpcServer(mServerConf, mServerUser, mExecutor);
    mServers.add(server);
    return server;
  }

  @Override
  public synchronized void close() {
    if (!mClosed) {
      mClosed = true;

      // Close created clients.
      List<CompletableFuture<Void>> clientCloseFutures = new ArrayList<>(mClients.size());
      for (CopycatGrpcClient client : mClients) {
        clientCloseFutures.add(client.close());
      }
      mClients.clear();
      try {
        CompletableFuture.allOf(clientCloseFutures.toArray(new CompletableFuture[0])).get();
      } catch (Exception e) {
        LOG.warn("Failed to close copycat transport clients.", e);
      }

      // Close created servers.
      List<CompletableFuture<Void>> serverCloseFutures = new ArrayList<>(mServers.size());
      for (CopycatGrpcServer server : mServers) {
        serverCloseFutures.add(server.close());
      }
      mServers.clear();
      try {
        CompletableFuture.allOf(serverCloseFutures.toArray(new CompletableFuture[0])).get();
      } catch (Exception e) {
        LOG.warn("Failed to close copycat transport servers.", e);
      }

      // Shut down transport executor.
      mExecutor.shutdownNow();
    }
  }
}
