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
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
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
  private ClientId mLocalClientId;
  private volatile RaftClient mClient;
  private boolean mEnableRemoteClient;

  /**
   * @param server the local raft server
   * @param clientSupplier a function for building a remote raft client
   * @param localClientId the client id for local requests
   * @param configuration the server configuration
   */
  public LocalFirstRaftClient(RaftServer server, Supplier<RaftClient> clientSupplier,
      ClientId localClientId, InstancedConfiguration configuration) {
    mServer = server;
    mClientSupplier = clientSupplier;
    mLocalClientId = localClientId;
    mEnableRemoteClient = configuration.getBoolean(
        PropertyKey.MASTER_EMBEDDED_JOURNAL_WRITE_REMOTE_ENABLED);
    if (mEnableRemoteClient && !configuration.getBoolean(
        PropertyKey.MASTER_EMBEDDED_JOURNAL_WRITE_LOCAL_FIRST_ENABLED)) {
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
    if (!mEnableRemoteClient || mClient == null) {
      return sendLocalRequest(message, timeout);
    } else {
      return sendRemoteRequest(message);
    }
  }

  private CompletableFuture<RaftClientReply> sendLocalRequest(Message message,
      TimeDuration timeout) throws IOException {
    LOG.trace("Sending local message {}", message);
    // ClientId, ServerId, and GroupId must not be null
    RaftClientRequest request = RaftClientRequest.newBuilder()
            .setClientId(mLocalClientId)
            .setServerId(mServer.getId())
            .setGroupId(RaftJournalSystem.RAFT_GROUP_ID)
            .setCallId(RaftJournalSystem.nextCallId())
            .setMessage(message)
            .setType(RaftClientRequest.writeRequestType())
            .setSlidingWindowEntry(null)
            .build();
    return mServer.submitClientRequestAsync(request)
        .thenApply(reply -> handleLocalException(message, reply, timeout));
  }

  private RaftClientReply handleLocalException(Message message, RaftClientReply reply,
      TimeDuration timeout) {
    LOG.trace("Message {} received reply {}", message, reply);
    if (mEnableRemoteClient && reply.getException() != null) {
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

  private void handleRemoteException(Throwable t) {
    if (t == null) {
      return;
    }
    LOG.trace("Received remote exception", t);
    if (t instanceof AlreadyClosedException
        || (t != null && t.getCause() instanceof AlreadyClosedException)) {
      // create a new client if the current client is already closed
      LOG.warn("Connection is closed. Creating a new client.");
      try {
        mClient.close();
      } catch (IOException e) {
        LogUtils.warnWithException(LOG, "Failed to close client: {}", e.toString());
      }
      mClient = mClientSupplier.get();
    }
  }

  private CompletableFuture<RaftClientReply> sendRemoteRequest(Message message) {
    ensureClient();
    LOG.trace("Sending remote message {}", message);
    return mClient.async().send(message).exceptionally(t -> {
      handleRemoteException(t);
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
