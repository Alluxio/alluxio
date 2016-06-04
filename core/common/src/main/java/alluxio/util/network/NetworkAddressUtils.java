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

package alluxio.util.network;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LeaderInquireClient;
import alluxio.util.OSUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.thrift.transport.TServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Common network address related utilities shared by all components in Alluxio.
 */
@ThreadSafe
public final class NetworkAddressUtils {
  public static final String WILDCARD_ADDRESS = "0.0.0.0";

  /**
   * Checks if the underlying OS is Windows.
   */
  public static final boolean WINDOWS = OSUtils.isWindows();
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static String sLocalHost;
  private static String sLocalIP;

  private NetworkAddressUtils() {}

  /**
   * Different types of services that client uses to connect. These types also indicate the service
   * bind address
   */
  public enum ServiceType {
    /**
     * Master RPC service (Thrift).
     */
    MASTER_RPC("Alluxio Master RPC service", Constants.MASTER_HOSTNAME, Constants.MASTER_BIND_HOST,
        Constants.MASTER_RPC_PORT, Constants.DEFAULT_MASTER_PORT),

    /**
     * Master web service (Jetty).
     */
    MASTER_WEB("Alluxio Master Web service", Constants.MASTER_WEB_HOSTNAME,
        Constants.MASTER_WEB_BIND_HOST, Constants.MASTER_WEB_PORT,
        Constants.DEFAULT_MASTER_WEB_PORT),

    /**
     * Worker RPC service (Thrift).
     */
    WORKER_RPC("Alluxio Worker RPC service", Constants.WORKER_HOSTNAME, Constants.WORKER_BIND_HOST,
        Constants.WORKER_RPC_PORT, Constants.DEFAULT_WORKER_PORT),

    /**
     * Worker data service (Netty).
     */
    WORKER_DATA("Alluxio Worker data service", Constants.WORKER_DATA_HOSTNAME,
        Constants.WORKER_DATA_BIND_HOST, Constants.WORKER_DATA_PORT,
        Constants.DEFAULT_WORKER_DATA_PORT),

    /**
     * Worker web service (Jetty).
     */
    WORKER_WEB("Alluxio Worker Web service", Constants.WORKER_WEB_HOSTNAME,
        Constants.WORKER_WEB_BIND_HOST, Constants.WORKER_WEB_PORT,
        Constants.DEFAULT_WORKER_WEB_PORT);

    // service name
    private final String mServiceName;

    // the key of connect hostname
    private final String mHostNameKey;

    // the key of bind hostname
    private final String mBindHostKey;

    // the key of service port
    private final String mPortKey;

    // default port number
    private final int mDefaultPort;

    ServiceType(String serviceName, String hostNameKey, String bindHostKey, String portKey,
        int defaultPort) {
      mServiceName = serviceName;
      mHostNameKey = hostNameKey;
      mBindHostKey = bindHostKey;
      mPortKey = portKey;
      mDefaultPort = defaultPort;
    }

    /**
     * Gets service name.
     *
     * @return service name
     */
    public String getServiceName() {
      return mServiceName;
    }

    /**
     * Gets the key of connect hostname.
     *
     * @return key of connect hostname
     */
    public String getHostNameKey() {
      return mHostNameKey;
    }

    /**
     * Gets the key of bind hostname.
     *
     * @return key of bind hostname
     */
    public String getBindHostKey() {
      return mBindHostKey;
    }

    /**
     * Gets the key of service port.
     *
     * @return key of service port
     */
    public String getPortKey() {
      return mPortKey;
    }

    /**
     * Gets the default port number on service.
     *
     * @return default port
     */
    public int getDefaultPort() {
      return mDefaultPort;
    }
  }

  /**
   * Checks if the given port is valid.
   *
   * @param port the port to check
   */
  public static void assertValidPort(final int port) {
    Preconditions.checkArgument(port < 65536, "Port must be less than 65536");
    Preconditions.checkArgument(port >= 0, "Port must be non-negative");
  }

  /**
   * Checks if the given port in the address is valid.
   *
   * @param address the {@link InetSocketAddress} with the port to check
   */
  public static void assertValidPort(final InetSocketAddress address) {
    assertValidPort(address.getPort());
  }

  /**
   * Helper method to get the {@link InetSocketAddress} address for client to communicate with the
   * service.
   *
   * @param service the service name used to connect
   * @param conf the configuration of Alluxio
   * @return the service address that a client (typically outside the service machine) uses to
   *         communicate with service.
   */
  public static InetSocketAddress getConnectAddress(ServiceType service, Configuration conf) {
    return new InetSocketAddress(getConnectHost(service, conf), getPort(service, conf));
  }

  /**
   * Provides an externally resolvable hostname for client to communicate with the service. If the
   * hostname is not explicitly specified, Alluxio will try to use the bind host. If the bind host
   * is wildcard, Alluxio will automatically determine an appropriate hostname from local machine.
   * The various possibilities shown in the following table:
   * <table>
   * <caption>Hostname Scenarios</caption>
   * <thead>
   * <tr>
   * <th>Specified Hostname</th>
   * <th>Specified Bind Host</th>
   * <th>Returned Connect Host</th>
   * </tr>
   * </thead>
   * <tbody>
   * <tr>
   * <td>hostname</td>
   * <td>hostname</td>
   * <td>hostname</td>
   * </tr>
   * <tr>
   * <td>not defined</td>
   * <td>hostname</td>
   * <td>hostname</td>
   * </tr>
   * <tr>
   * <td>hostname</td>
   * <td>0.0.0.0 or not defined</td>
   * <td>hostname</td>
   * </tr>
   * <tr>
   * <td>not defined</td>
   * <td>0.0.0.0 or not defined</td>
   * <td>localhost</td>
   * </tr>
   * </tbody>
   * </table>
   *
   * @param service Service type used to connect
   * @param conf Alluxio configuration used to look up the host resolution timeout
   * @return the externally resolvable hostname that the client can use to communicate with the
   *         service.
   */
  public static String getConnectHost(ServiceType service, Configuration conf) {
    if (conf.containsKey(service.mHostNameKey)) {
      String connectHost = conf.get(service.mHostNameKey);
      if (!connectHost.isEmpty() && !connectHost.equals(WILDCARD_ADDRESS)) {
        return connectHost;
      }
    }
    if (conf.containsKey(service.mBindHostKey)) {
      String bindHost = conf.get(service.mBindHostKey);
      if (!bindHost.isEmpty() && !bindHost.equals(WILDCARD_ADDRESS)) {
        return bindHost;
      }
    }
    return getLocalHostName(conf);
  }

  /**
   * Gets the port number on a given service type. If user defined port number is not explicitly
   * specified, Alluxio will use the default service port.
   *
   * @param service Service type used to connect
   * @param conf Alluxio configuration
   * @return the service port number
   */
  public static int getPort(ServiceType service, Configuration conf) {
    return conf.getInt(service.mPortKey);
  }

  /**
   * Helper method to get the {@link InetSocketAddress} bind address on a given service.
   * <p>
   * Host bind information searching order:
   * <ol>
   * <li>System properties or environment variables via alluxio-env.sh
   * <li>Default properties via alluxio-default.properties file
   * <li>A externally resolvable local hostname for the host this JVM is running on
   * </ol>
   *
   * @param service the service name used to connect
   * @param conf the configuration of Alluxio
   * @return the InetSocketAddress the service will bind to
   */
  public static InetSocketAddress getBindAddress(ServiceType service, Configuration conf) {
    int port = getPort(service, conf);
    assertValidPort(port);

    String host;
    if (conf.containsKey(service.mBindHostKey) && !conf.get(service.mBindHostKey).isEmpty()) {
      host = conf.get(service.mBindHostKey);
    } else {
      host = getLocalHostName(conf);
    }
    return new InetSocketAddress(host, port);
  }

  /**
   * Gets a local host name for the host this JVM is running on.
   *
   * @param conf Alluxio configuration used to look up the host resolution timeout
   * @return the local host name, which is not based on a loopback ip address
   */
  public static synchronized String getLocalHostName(Configuration conf) {
    if (sLocalHost != null) {
      return sLocalHost;
    }
    int hostResolutionTimeout = conf.getInt(Constants.NETWORK_HOST_RESOLUTION_TIMEOUT_MS);
    return getLocalHostName(hostResolutionTimeout);
  }

  /**
   * Gets a local host name for the host this JVM is running on.
   *
   * @param timeout Timeout in milliseconds to use for checking that a possible local host is
   *        reachable
   * @return the local host name, which is not based on a loopback ip address
   */
  public static synchronized String getLocalHostName(int timeout) {
    if (sLocalHost != null) {
      return sLocalHost;
    }

    try {
      sLocalHost = InetAddress.getByName(getLocalIpAddress(timeout)).getCanonicalHostName();
      return sLocalHost;
    } catch (UnknownHostException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Gets a local IP address for the host this JVM is running on.
   *
   * @param conf Alluxio configuration
   * @return the local ip address, which is not a loopback address and is reachable
   */
  public static synchronized String getLocalIpAddress(Configuration conf) {
    if (sLocalIP != null) {
      return sLocalIP;
    }
    int hostResolutionTimeout = conf.getInt(Constants.NETWORK_HOST_RESOLUTION_TIMEOUT_MS);
    return getLocalIpAddress(hostResolutionTimeout);
  }

  /**
   * Gets a local IP address for the host this JVM is running on.
   *
   * @param timeout Timeout in milliseconds to use for checking that a possible local IP is
   *        reachable
   * @return the local ip address, which is not a loopback address and is reachable
   */
  public static synchronized String getLocalIpAddress(int timeout) {
    if (sLocalIP != null) {
      return sLocalIP;
    }

    try {
      InetAddress address = InetAddress.getLocalHost();
      LOG.debug("address: {} isLoopbackAddress: {}, with host {} {}", address,
          address.isLoopbackAddress(), address.getHostAddress(), address.getHostName());

      // Make sure that the address is actually reachable since in some network configurations
      // it is possible for the InetAddress.getLocalHost() call to return a non-reachable
      // address e.g. a broadcast address
      if (!isValidAddress(address, timeout)) {
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();

        // Make getNetworkInterfaces have the same order of network interfaces as listed on
        // unix-like systems. This optimization can help avoid to get some special addresses, such
        // as loopback address"127.0.0.1", virtual bridge address "192.168.122.1" as far as
        // possible.
        if (!WINDOWS) {
          List<NetworkInterface> netIFs = Collections.list(networkInterfaces);
          Collections.reverse(netIFs);
          networkInterfaces = Collections.enumeration(netIFs);
        }

        while (networkInterfaces.hasMoreElements()) {
          NetworkInterface ni = networkInterfaces.nextElement();
          Enumeration<InetAddress> addresses = ni.getInetAddresses();
          while (addresses.hasMoreElements()) {
            address = addresses.nextElement();

            // Address must not be link local or loopback. And it must be reachable
            if (isValidAddress(address, timeout)) {
              sLocalIP = address.getHostAddress();
              return sLocalIP;
            }
          }
        }

        LOG.warn("Your hostname, " + InetAddress.getLocalHost().getHostName() + " resolves to"
            + " a loopback/non-reachable address: " + address.getHostAddress()
            + ", but we couldn't find any external IP address!");
      }

      sLocalIP = address.getHostAddress();
      return sLocalIP;
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * @param host the host to try to connect to
   * @param port the port to try to connect on
   * @return whether a socket connection can be made to the given host on the given port
   */
  public static boolean isServing(String host, int port) {
    if (port < 0) {
      return false;
    }
    try {
      Socket socket = new Socket(host, port);
      socket.close();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Tests if the address is externally resolvable. Address must not be wildcard, link local,
   * loopback address, non-IPv4, or other unreachable addresses.
   *
   * @param address The testing address
   * @param timeout Timeout in milliseconds to use for checking that a possible local IP is
   *        reachable
   * @return a {@code boolean} indicating if the given address is externally resolvable address
   * @throws IOException if the address resolution fails
   */
  private static boolean isValidAddress(InetAddress address, int timeout) throws IOException {
    return !address.isAnyLocalAddress() && !address.isLinkLocalAddress()
        && !address.isLoopbackAddress() && address.isReachable(timeout)
        && (address instanceof Inet4Address);
  }

  /**
   * Replaces and resolves the hostname in a given address or path string.
   *
   * @param path an address or path string, e.g., "hdfs://host:port/dir", "file:///dir", "/dir"
   * @return an address or path string with hostname resolved, or the original path intact if no
   *         hostname is embedded, or null if the given path is null or empty.
   * @throws UnknownHostException if the hostname cannot be resolved
   */
  public static AlluxioURI replaceHostName(AlluxioURI path) throws UnknownHostException {
    if (path == null) {
      return null;
    }

    if (path.hasAuthority()) {
      String authority = resolveHostName(path.getHost());
      if (path.getPort() != -1) {
        authority += ":" + path.getPort();
      }
      return new AlluxioURI(path.getScheme(), authority, path.getPath(), path.getQueryMap());
    }
    return path;
  }

  /**
   * Resolves a given hostname by a canonical hostname. When a hostname alias (e.g., those specified
   * in /etc/hosts) is given, the alias may not be resolvable on other hosts in a cluster unless the
   * same alias is defined there. In this situation, loadufs would break.
   *
   * @param hostname the input hostname, which could be an alias
   * @return the canonical form of the hostname, or null if it is null or empty
   * @throws UnknownHostException if the given hostname cannot be resolved
   */
  public static String resolveHostName(String hostname) throws UnknownHostException {
    if (hostname == null || hostname.isEmpty()) {
      return null;
    }

    return InetAddress.getByName(hostname).getCanonicalHostName();
  }

  /**
   * Gets FQDN(Full Qualified Domain Name) from Java representations of network address, except
   * String representation which should be handled by {@link #resolveHostName(String)} which will
   * handle the situation where hostname is null.
   *
   * @param addr the input network address representation, can not be null
   * @return the resolved FQDN host name
   */
  public static String getFqdnHost(InetSocketAddress addr) {
    Preconditions.checkNotNull(addr.getAddress(), "the address of " + addr + " is invalid.");
    return addr.getAddress().getCanonicalHostName();
  }

  /**
   * Gets FQDN(Full Qualified Domain Name) from Alluxio representation of network address.
   *
   * @param addr the input network address representation
   * @return the resolved FQDN host name
   * @throws UnknownHostException if the host is not known
   */
  public static String getFqdnHost(WorkerNetAddress addr) throws UnknownHostException {
    return resolveHostName(addr.getHost());
  }

  /**
   * Gets the port for the underline socket. This function calls
   * {@link #getThriftSocket(org.apache.thrift.transport.TServerSocket)}, so reflection will be used
   * to get the port.
   *
   * @param thriftSocket the underline socket
   * @return the thrift port for the underline socket
   * @see #getThriftSocket(org.apache.thrift.transport.TServerSocket)
   */
  public static int getThriftPort(TServerSocket thriftSocket) {
    return getThriftSocket(thriftSocket).getLocalPort();
  }

  /**
   * Extracts the port from the thrift socket. As of thrift 0.9, the internal socket used is not
   * exposed in the API, so this function will use reflection to get access to it.
   *
   * @param thriftSocket the underline thrift socket
   * @return the server socket
   */
  public static ServerSocket getThriftSocket(final TServerSocket thriftSocket) {
    try {
      Field field = TServerSocket.class.getDeclaredField("serverSocket_");
      field.setAccessible(true);
      return (ServerSocket) field.get(thriftSocket);
    } catch (NoSuchFieldException e) {
      throw Throwables.propagate(e);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Parses {@link InetSocketAddress} from a String.
   *
   * @param address socket address to parse
   * @return InetSocketAddress of the String
   * @throws IOException if the socket address is invalid
   */
  public static InetSocketAddress parseInetSocketAddress(String address) throws IOException {
    if (address == null) {
      return null;
    }
    String[] strArr = address.split(":");
    if (strArr.length != 2) {
      throw new IOException("Invalid InetSocketAddress " + address);
    }
    return new InetSocketAddress(strArr[0], Integer.parseInt(strArr[1]));
  }

  /**
   * Extracts rpcPort InetSocketAddress from Alluxio representation of network address.
   *
   * @param netAddress the input network address representation
   * @return InetSocketAddress
   */
  public static InetSocketAddress getRpcPortSocketAddress(WorkerNetAddress netAddress) {
    try {
      String host = getFqdnHost(netAddress);
      int port = netAddress.getRpcPort();
      return new InetSocketAddress(host, port);
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Extracts dataPort InetSocketAddress from Alluxio representation of network address.
   *
   * @param netAddress the input network address representation
   * @return InetSocketAddress
   */
  public static InetSocketAddress getDataPortSocketAddress(WorkerNetAddress netAddress) {
    try {
      String host = getFqdnHost(netAddress);
      int port = netAddress.getDataPort();
      return new InetSocketAddress(host, port);
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Get the active master address from zookeeper for the fault tolerant Alluxio masters.
   * The zookeeper path is specified by the config: {@link Constants#ZOOKEEPER_LEADER_PATH}.
   *
   * @param conf the Alluxio configuration
   * @return InetSocketAddress the active master address retrieved from zookeeper
   */
  public static InetSocketAddress getMasterAddressFromZK(Configuration conf) {
    Preconditions.checkState(conf.containsKey(Constants.ZOOKEEPER_ADDRESS));
    Preconditions.checkState(conf.containsKey(Constants.ZOOKEEPER_LEADER_PATH));
    LeaderInquireClient leaderInquireClient =
            LeaderInquireClient.getClient(conf.get(Constants.ZOOKEEPER_ADDRESS),
                    conf.get(Constants.ZOOKEEPER_LEADER_PATH), conf);
    try {
      String temp = leaderInquireClient.getMasterAddress();
      return NetworkAddressUtils.parseInetSocketAddress(temp);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }
}
