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

package alluxio.network.thrift;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

/**
 * Util class for methods to create Thrift communication with Alluxio services.
 */
public final class ThriftUtils {
  /** Timeout for client socket in ms. */
  private static final int SOCKET_TIMEOUT_MS = (int) Configuration
      .getMs(PropertyKey.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS);
  /** Timeout for server socket in ms. */
  private static final int SERVER_SOCKET_TIMEOUT_MS = (int) Configuration
      .getMs(PropertyKey.MASTER_CONNECTION_TIMEOUT_MS);

  /**
   * @param transport a transport for communicating with an Alluxio Thrift server
   * @param serviceName the service to communicate with
   * @return a Thrift protocol for communicating with the given service through the transport
   */
  public static TProtocol createThriftProtocol(TTransport transport, String serviceName)
      throws UnauthenticatedException {
    TProtocol binaryProtocol = new TBinaryProtocol(transport);
    TProtocol multiplexedProtocol = new TMultiplexedProtocol(binaryProtocol, serviceName);
    return multiplexedProtocol;
  }

  /**
   * @return a Thrift protocol factory for communicating with server through the transport
   */
  public static TProtocolFactory createThriftProtocolFactory() {
    return new TBinaryProtocol.Factory(true, true);
  }

  /**
   * Creates a new Thrift socket that will connect to the given address.
   *
   * @param address The given address to connect
   * @return An unconnected socket
   */
  public static TSocket createThriftSocket(InetSocketAddress address) {
    return new TSocket(address.getHostName(), address.getPort(), SOCKET_TIMEOUT_MS);
  }

  /**
   * Creates a new Thrift server socket that listen on the given address.
   *
   * @param address The given address to listen on
   * @return A server socket
   */
  public static TServerSocket createThriftServerSocket(InetSocketAddress address)
      throws TTransportException {
    // The socket tracking socket will close all client sockets when the server socket is closed.
    // This is necessary so that clients don't receive spurious errors during failover. The master
    // will close this socket before resetting its state during stepdown.
    return new SocketTrackingTServerSocket(address, SERVER_SOCKET_TIMEOUT_MS);
  }

  /**
   * Gets the port for the underline socket.
   *
   * @param thriftSocket the underline socket
   * @return the thrift port for the underline socket
   */
  public static int getThriftPort(TServerSocket thriftSocket) {
    return getThriftSocket(thriftSocket).getLocalPort();
  }

  /**
   * Extracts internal socket from the thrift socket. As of thrift 0.9, the internal socket used is
   * not exposed in the API, so this function will use reflection to get access to it.
   *
   * @param thriftSocket the underline thrift socket
   * @return the server socket
   */
  public static ServerSocket getThriftSocket(final TServerSocket thriftSocket) {
    try {
      Field field = TServerSocket.class.getDeclaredField("serverSocket_");
      field.setAccessible(true);
      return (ServerSocket) field.get(thriftSocket);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private ThriftUtils() {} // not intended for instantiation
}
