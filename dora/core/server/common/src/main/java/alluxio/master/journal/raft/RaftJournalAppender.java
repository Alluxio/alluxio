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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.LogUtils;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.server.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/**
 * A client to append messages to RAFT log state.
 */
public class RaftJournalAppender implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RaftJournalAppender.class);
  private static final boolean MASTER_EMBEDDED_JOURNAL_WRITE_REMOTE_ENABLED =
      Configuration.getBoolean(PropertyKey.MASTER_EMBEDDED_JOURNAL_WRITE_REMOTE_ENABLED);
  /** Hosting server for the appender. Used by default for appending log entries. */
  private final RaftServer mServer;
  /** local client ID, provided along with hosting server.  */
  private final ClientId mLocalClientId;
  /** Remote raft client. */
  private final Supplier<RaftClient> mClientSupplier;
  private volatile RaftClient mClient;

  /**
   * @param server the local raft server
   * @param clientSupplier a function for building a remote raft client
   * @param localClientId the client id for local requests
   */
  public RaftJournalAppender(RaftServer server, Supplier<RaftClient> clientSupplier,
      ClientId localClientId) {
    mServer = Objects.requireNonNull(server, "RaftServer is null");
    mClientSupplier = Objects.requireNonNull(clientSupplier, "clientSupplier is null");
    mLocalClientId = Objects.requireNonNull(localClientId, "clientId is null");
  }

  /**
   * Sends a request to raft server asynchronously.
   * @param message the message to send
   * @return a future of the server reply
   * @throws IOException if an exception occurred while sending the request
   */
  public CompletableFuture<RaftClientReply> sendAsync(Message message) throws IOException {
    if (MASTER_EMBEDDED_JOURNAL_WRITE_REMOTE_ENABLED) {
      return sendRemoteRequest(message);
    } else {
      return sendLocalRequest(message);
    }
  }

  private CompletableFuture<RaftClientReply> sendLocalRequest(Message message) throws IOException {
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
    return mServer.submitClientRequestAsync(request);
  }

  private CompletableFuture<RaftClientReply> sendRemoteRequest(Message message) {
    ensureClient();
    LOG.trace("Sending remote message {}", message);
    return mClient.async().send(message).exceptionally(t -> {
      // Handle and rethrow exception.
      handleRemoteException(t);
      throw new CompletionException(t.getCause());
    });
  }

  private void ensureClient() {
    if (mClient == null) {
      mClient = mClientSupplier.get();
    }
  }

  private void handleRemoteException(Throwable t) {
    if (t == null) {
      return;
    }
    LOG.trace("Received remote exception", t);
    if (t instanceof AlreadyClosedException || t.getCause() instanceof AlreadyClosedException) {
      // create a new client if the current client is already closed
      LOG.warn("Connection is closed. Closing ratis client.");
      try {
        mClient.close();
      } catch (IOException e) {
        LogUtils.warnWithException(LOG, "Failed to close client: {}", e.toString());
      } finally {
        mClient = null;
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (mClient != null) {
      mClient.close();
    }
  }
}
