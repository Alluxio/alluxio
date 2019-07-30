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

import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.security.user.ServerUserState;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.concurrent.SingleThreadContext;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.transport.Transport;
import io.atomix.copycat.protocol.ClientRequestTypeResolver;
import io.atomix.copycat.protocol.ClientResponseTypeResolver;
import io.atomix.copycat.server.storage.util.StorageSerialization;
import io.atomix.copycat.server.util.ServerSerialization;
import io.atomix.copycat.util.ProtocolSerialization;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Units tests for {@link CopycatGrpcTransport}.
 */
public class CopycatGrpcTransportTest {

  private Transport mTransport;

  @Before
  public void before() {
    mTransport = new CopycatGrpcTransport(ServerConfiguration.global(), ServerUserState.global());
  }

  @After
  public void after() {
    mTransport.close();
  }

  @Test
  public void testEstablishConnection() throws Exception {
    // Set by server connection listener, when new connection is opened to server.
    final AtomicBoolean connectionEstablished = new AtomicBoolean(false);
    // Server connection lister that validates connection establishment.
    Consumer<Connection> connectionListener = new CopycatTransportTestListener((connection) -> {
      // Set connection as established.
      connectionEstablished.set(true);
    });

    // Catalyst thread context for managing client/server.
    ThreadContext connectionContext = createSingleThreadContext("ClientServerCtx");

    // Create and bind transport server.
    Address address = bindServer(connectionContext, mTransport.server(), connectionListener);

    // Open a client connection to server.
    connectClient(connectionContext, mTransport.client(), address);

    // Assert server has established the connection.
    Assert.assertTrue(connectionEstablished.get());
  }

  @Test
  public void testConnectionIsolation() throws Exception {

    // Catalyst thread context for managing client/server.
    ThreadContext connectionContext = createSingleThreadContext("ClientServerCtx");

    // Create and bind transport server.
    Address address =
        bindServer(connectionContext, mTransport.server(), new CopycatTransportTestListener());

    Client transportClient = mTransport.client();
    // Open 2 client connections to server.
    Connection clientConnection1 = connectClient(connectionContext, transportClient, address);
    Connection clientConnection2 = connectClient(connectionContext, transportClient, address);

    // Close connection-1.
    clientConnection1.close().get();

    // Sent request over connection-2. Assert response present and null.
    Assert.assertNull(sendRequest(clientConnection2, new DummyRequest("dummy")).get());
  }

  @Test
  public void testConnectionClosed() throws Exception {

    // Catalyst thread context for managing client/server.
    ThreadContext connectionContext = createSingleThreadContext("ClientServerCtx");

    // Create and bind transport server.
    Address address =
        bindServer(connectionContext, mTransport.server(), new CopycatTransportTestListener());

    // Open a client connection to server.
    Connection clientConnection = connectClient(connectionContext, mTransport.client(), address);

    // Close connection.
    clientConnection.close().get();

    // Sent request over connection. Assert request can't be sent over closed connection.
    boolean failed = false;
    try {
      sendRequest(clientConnection, new DummyRequest("dummy")).get();
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalStateException);
      failed = true;
    }
    Assert.assertTrue(failed);
  }

  @Test
  public void testServerClosed() throws Exception {

    // Catalyst thread context for managing client/server.
    ThreadContext connectionContext = createSingleThreadContext("ClientServerCtx");

    // Create transport server.
    Server server = mTransport.server();
    // Bind transport server.
    Address address = bindServer(connectionContext, server, new CopycatTransportTestListener());

    // Open a client connection to server.
    Connection clientConnection = connectClient(connectionContext, mTransport.client(), address);

    // Close server.
    server.close();

    // Sent request over connection. Assert request can't be sent over closed connection.
    boolean failed = false;
    try {
      sendRequest(clientConnection, new DummyRequest("dummy")).get();
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalStateException);
      failed = true;
    }
    Assert.assertTrue(failed);
  }

  /**
   * Creates and binds transport server.
   *
   * @param context catalyst context
   * @param listener listener for new connections
   * @return address to which the server is bound
   * @throws Exception
   */
  private Address bindServer(ThreadContext context, Server server, Consumer<Connection> listener)
      throws Exception {

    ServerSocket autoBindSocket = new ServerSocket(0);
    Address serverAddress = new Address("localhost", autoBindSocket.getLocalPort());
    autoBindSocket.close();

    context.execute(() -> {
      try {
        // Bind server.
        server.listen(serverAddress, listener).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).get();

    return serverAddress;
  }

  /**
   * Opens a client connection to server.
   *
   * @param context catalyst context
   * @param client transport client
   * @param serverAddress server address
   * @return client connection
   * @throws Exception
   */
  private Connection connectClient(ThreadContext context, Client client, Address serverAddress)
      throws Exception {
    AtomicReference<Connection> clientConnectionRef = new AtomicReference<>(null);
    context.execute(() -> {
      try {
        // Create client connection to server.
        clientConnectionRef.set(client.connect(serverAddress).get());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).get();

    /*
     * gRPC won't establish stream until message is sent over. Explicitly send a command to cause
     * underlying gRPC stream to be established.
     *
     */

    // Validate connection.
    Assert.assertNull(sendRequest(clientConnectionRef.get(), new DummyRequest("dummy")).get());

    return clientConnectionRef.get();
  }

  /**
   * Sends a request over connection.
   *
   * @param connection connection for sending
   * @param request request to send
   * @return response to request
   * @throws Exception
   */
  private CompletableFuture<Object> sendRequest(Connection connection, Object request)
      throws Exception {
    // Future for receiving command completion.
    CompletableFuture<Object> commandFuture = new CompletableFuture<>();
    // Sent a dummy client request.
    createSingleThreadContext("CommandCtx").execute(() -> {
      connection.sendAndReceive(request).whenComplete((result, error) -> {
        if (error != null) {
          commandFuture.completeExceptionally(error);
        } else {
          commandFuture.complete(result);
        }
      });
    }).get();
    return commandFuture;
  }

  /**
   * Creates test serializer.
   *
   * @return the serializer
   */
  private Serializer createTestSerializer() {
    Serializer serializer = RaftJournalSystem.createSerializer();
    serializer.resolve(new ClientRequestTypeResolver());
    serializer.resolve(new ClientResponseTypeResolver());
    serializer.resolve(new ProtocolSerialization());
    serializer.resolve(new ServerSerialization());
    serializer.resolve(new StorageSerialization());

    // Register dummy test command.
    serializer.register(DummyRequest.class);

    return serializer;
  }

  /**
   * Create a single threaded catalyst context with test serializer.
   *
   * @param contextName context name
   * @return thread context
   */
  private ThreadContext createSingleThreadContext(String contextName) {
    return new SingleThreadContext(contextName, createTestSerializer());
  }

  /**
   * Dummy request class that keeps a single string value.
   *
   * Note: Defined as "public static" for allowing catalyst to see empty constructor.
   */
  public static class DummyRequest implements CatalystSerializable {
    private String mContent;

    /**
     * Required by catalyst.
     */
    public DummyRequest() {}

    /**
     * Creates dummy request with given content.
     *
     * @param content request content
     */
    public DummyRequest(String content) {
      mContent = content;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      byte[] contentBytes = mContent.getBytes();
      buffer.writeInt(contentBytes.length);
      buffer.writeBytes(contentBytes);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      mContent = new String(buffer.readBytes(buffer.readInt()));
    }
  }

  /**
   * Test listener that is used to register test request types on server connection.
   */
  class CopycatTransportTestListener implements Consumer<Connection> {
    Consumer<Connection> mNestedListener;

    /**
     * Creates test listener.
     */
    public CopycatTransportTestListener() {
      this(null);
    }

    /**
     * Creates test listener with nested listener.
     *
     * @param nestedListener nested listener
     */
    public CopycatTransportTestListener(Consumer<Connection> nestedListener) {
      mNestedListener = nestedListener;
    }

    @Override
    public void accept(Connection connection) {
      // Register request handler for 'DummyRequest'.
      connection.handler(DummyRequest.class, (command) -> {
        return CompletableFuture.completedFuture(null);
      });
      if (mNestedListener != null) {
        mNestedListener.accept(connection);
      }
    }
  }
}
