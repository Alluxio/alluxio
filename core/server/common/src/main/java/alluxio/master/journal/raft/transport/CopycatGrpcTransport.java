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

import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.transport.Transport;

import java.util.LinkedList;
import java.util.List;

/**
 * Copycat {@link Transport} implementation that uses Alluxio gRPC.
 */
public class CopycatGrpcTransport implements Transport {

  /** Alluxio configuration for clients. */
  private AlluxioConfiguration mClientConf;
  /** Alluxio configuration for servers. */
  private AlluxioConfiguration mServerConf;
  /** User for clients. */
  private UserState mClientUser;
  /** User for servers. */
  private UserState mServerUser;

  /** List of created clients. */
  private List<CopycatGrpcClient> mClients = new LinkedList<>();
  /** List of created servers. */
  private List<CopycatGrpcServer> mServers = new LinkedList<>();

  /** Whether the transport is closed. */
  private boolean mClosed;

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
  }

  @Override
  public synchronized Client client() {
    if (mClosed) {
      throw new RuntimeException("Transport closed");
    }
    CopycatGrpcClient client = new CopycatGrpcClient(mClientConf, mClientUser);
    mClients.add(client);
    return client;
  }

  @Override
  public synchronized Server server() {
    if (mClosed) {
      throw new RuntimeException("Transport closed");
    }
    CopycatGrpcServer server = new CopycatGrpcServer(mServerConf, mServerUser);
    mServers.add(server);
    return server;
  }

  @Override
  public synchronized void close() {
    if (!mClosed) {
      mClosed = true;

      // Close created clients.
      for (CopycatGrpcClient client : mClients) {
        client.close();
      }
      mClients.clear();

      // Close created servers.
      for (CopycatGrpcServer server : mServers) {
        server.close();
      }
      mServers.clear();
    }
  }
}
