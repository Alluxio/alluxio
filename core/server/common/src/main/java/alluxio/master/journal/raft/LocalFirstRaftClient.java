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

package alluxio.master.journal.raft;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.LogUtils;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.AlreadyClosedException;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * A client to send messages to a raft server with a strategy to attempt sending them locally first.
 */
public class LocalFirstRaftClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFirstRaftClient.class);
  private final RaftServer mServer;
  private final Supplier<RaftClient> mClientSupplier;
  private ClientId mClientId;
  private volatile RaftClient mClient;

  /**
   * @param server the local raft server
   * @param clientSupplier a function for building a remote raft client
   * @param clientId the client id
   * @param configuration the server configuration
   */
  public LocalFirstRaftClient(RaftServer server, Supplier<RaftClient> clientSupplier,
      ClientId clientId, InstancedConfiguration configuration) {
    mServer = server;
    mClientSupplier = clientSupplier;
    mClientId = clientId;
    if (!configuration.getBoolean(PropertyKey.MASTER_EMBEDDED_JOURNAL_WRITE_LOCAL_FIRST_ENABLED)) {
      ensureClient();
    }
  }

  /**
   * Sends a request to raft server asynchronously.
   * @param message the message to send
   * @param timeout the time duration to wait before giving up on the request
   * @return a future of the server reply
   * @throws IOException if an exception occured while sending the request
   */
  public CompletableFuture<RaftClientReply> sendAsync(Message message,
      TimeDuration timeout) throws IOException {
    if (mClient == null) {
      return sendLocalRequest(message, timeout);
    } else {
      return sendRemoteRequest(message);
    }
  }

  private CompletableFuture<RaftClientReply> sendLocalRequest(Message message,
      TimeDuration timeout) throws IOException {
    return mServer.submitClientRequestAsync(
        new RaftClientRequest(mClientId, null, RaftJournalSystem.RAFT_GROUP_ID,
            RaftJournalSystem.nextCallId(), message, RaftClientRequest.writeRequestType(), null))
        .thenApply(reply -> handleLocalException(message, reply, timeout));
  }

  private RaftClientReply handleLocalException(Message message, RaftClientReply reply,
      TimeDuration timeout) {
    if (reply.getException() != null) {
      if (reply.getException() instanceof NotLeaderException) {
        LOG.info("Local master is no longer a leader, falling back to remote client.");
        try {
          return sendRemoteRequest(message).get(timeout.getDuration(), timeout.getUnit());
        } catch (InterruptedException | TimeoutException e) {
          throw new CompletionException(e);
        } catch (ExecutionException e) {
          throw new CompletionException(e.getCause());
        }
      }
      throw new CompletionException(reply.getException());
    }
    return reply;
  }

  private CompletableFuture<RaftClientReply> sendRemoteRequest(Message message) {
    ensureClient();
    return mClient.sendAsync(message).exceptionally(t -> {
      if (t instanceof ExecutionException && t.getCause() instanceof AlreadyClosedException) {
        // create a new client if the current client is already closed
        try {
          mClient.close();
        } catch (IOException e) {
          LogUtils.warnWithException(LOG, "Failed to close client: {}", e.getMessage());
        }
        mClient = mClientSupplier.get();
      }
      throw new CompletionException(t.getCause());
    });
  }

  private void ensureClient() {
    if (mClient == null) {
      mClient = mClientSupplier.get();
    }
  }

  @Override
  public void close() throws IOException {
    if (mClient != null) {
      mClient.close();
    }
  }
}
