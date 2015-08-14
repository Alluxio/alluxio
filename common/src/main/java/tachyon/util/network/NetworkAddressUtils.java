/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.util.network;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.apache.thrift.transport.TServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.thrift.NetAddress;

/**
 * Common network address related utilities shared by all components in Tachyon.
 */
public final class NetworkAddressUtils {
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
     * Master RPC service (Thrift)
     */
    MASTER_RPC("Tachyon Master RPC service", Constants.MASTER_HOSTNAME, Constants.MASTER_BIND_HOST,
        Constants.MASTER_PORT, Constants.DEFAULT_MASTER_PORT),

    /**
     * Master web service (Jetty)
     */
    MASTER_WEB("Tachyon Master Web service", Constants.MASTER_WEB_HOSTNAME,
        Constants.MASTER_WEB_BIND_HOST, Constants.MASTER_WEB_PORT,
        Constants.DEFAULT_MASTER_WEB_PORT),

    /**
     * Worker RPC service (Thrift)
     */
    WORKER_RPC("Tachyon Worker RPC service", Constants.WORKER_HOSTNAME, Constants.WORKER_BIND_HOST,
        Constants.WORKER_PORT, Constants.DEFAULT_WORKER_PORT),

    /**
     * Worker data service (Netty)
     */
    WORKER_DATA("Tachyon Worker data service", Constants.WORKER_DATA_HOSTNAME,
        Constants.WORKER_DATA_BIND_HOST, Constants.WORKER_DATA_PORT,
        Constants.DEFAULT_WORKER_DATA_PORT),

    /**
     * Worker web service (Jetty)
     */
    WORKER_WEB("Tachyon Worker Web service", Constants.WORKER_WEB_HOSTNAME,
        Constants.WORKER_WEB_BIND_HOST, Constants.WORKER_WEB_PORT,
        Constants.DEFAULT_WORKER_WEB_PORT);

    // service name
    public final String mServiceName;

    // the key of connect hostname
    public final String mHostNameKey;

    // the key of bind hostname
    public final String mBindHostKey;

    // the key of port
    public final String mPortKey;

    // default port number
    public final int mDefaultPort;

    ServiceType(String serviceName, String hostNameKey, String bindHostKey, String portKey,
        int defaultPort) {
      this.mServiceName = serviceName;
      this.mHostNameKey = hostNameKey;
      this.mBindHostKey = bindHostKey;
      this.mPortKey = portKey;
      this.mDefaultPort = defaultPort;
    }
  }

  /**
   * Gets service connection hostname. If the connection hostname is not explicitly specified,
   * Tachyon will try bind hostname. If the bind hostname is wildcard, Tachyon will automatically
   * select an appropriate local hostname.
   *
   * @param service Service type used to connect
   * @param conf Tachyon configuration used to look up the host resolution timeout
   * @return the connection hostname that a client can use to reach the service.
   */
  public static String getConnectHost(ServiceType service, TachyonConf conf) {
    String connectHost = conf.get(service.mHostNameKey, "");
    String bindHost = conf.get(service.mBindHostKey, "");

    if (!connectHost.equals("0.0.0.0") && !connectHost.isEmpty()) {
      return connectHost;
    } else if (!bindHost.equals("0.0.0.0") && !bindHost.isEmpty()) {
      return bindHost;
    } else {
      return getLocalHostName(conf);
    }
  }

  /**
   * Helper method to get the {@link InetSocketAddress} connection address on a given service.
   *
   * @param service the service name used to connect
   * @param conf the configuration of Tachyon
   * @return a connection endpoint that a client uses to communicate with service.
   */
  public static InetSocketAddress getConnectAddress(ServiceType service, TachyonConf conf) {
    return new InetSocketAddress(getConnectHost(service, conf), getPort(service, conf));
  }

  /**
   * Helper method to get the {@link InetSocketAddress} bind address on a given service.
   * <p>
   * Host binding strategy on multihomed networks:
   * <ol>
   * <li>Environment variables via tachyon-env.sh or from OS settings
   * <li>Default properties via tachyon-default.properties file
   * <li>A reachable local host name for the host this JVM is running on
   * </ol>
   *
   * @param service the service name used to connect
   * @param conf the configuration of Tachyon
   * @return the {@link InetSocketAddress} the service will bind to
   */
  public static InetSocketAddress getBindAddress(ServiceType service, TachyonConf conf) {
    String host = conf.get(service.mBindHostKey, "");
    int port = getPort(service, conf);
    TachyonConf.assertValidPort(port, conf);

    if (!host.isEmpty()) {
      return new InetSocketAddress(host, port);
    } else {
      return new InetSocketAddress(getLocalHostName(conf), port);
    }
  }

  /**
   * Gets a local host name for the host this JVM is running on
   *
   * @param conf Tachyon configuration used to look up the host resolution timeout
   * @return the local host name, which is not based on a loopback ip address.
   */
  public static String getLocalHostName(TachyonConf conf) {
    if (sLocalHost != null) {
      return sLocalHost;
    }
    int hostResolutionTimeout = conf.getInt(Constants.HOST_RESOLUTION_TIMEOUT_MS,
        Constants.DEFAULT_HOST_RESOLUTION_TIMEOUT_MS);
    return getLocalHostName(hostResolutionTimeout);
  }

  /**
   * Gets a local host name for the host this JVM is running on
   *
   * @param timeout Timeout in milliseconds to use for checking that a possible local
   *                host is reachable
   * @return the local host name, which is not based on a loopback ip address.
   */
  public static String getLocalHostName(int timeout) {
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
   * Check if the underlying OS is Windows.
   */
  public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

  /**
   * Gets a local IP address for the host this JVM is running on
   *
   * @param conf Tachyon configuration
   * @return the local ip address, which is not a loopback address and is reachable
   */
  public static String getLocalIpAddress(TachyonConf conf) {
    if (sLocalIP != null) {
      return sLocalIP;
    }
    int hostResolutionTimeout = conf.getInt(Constants.HOST_RESOLUTION_TIMEOUT_MS,
        Constants.DEFAULT_HOST_RESOLUTION_TIMEOUT_MS);
    return getLocalIpAddress(hostResolutionTimeout);
  }

  /**
   * Gets a local IP address for the host this JVM is running on
   *
   * @param timeout Timeout in milliseconds to use for checking that a possible local IP is
   *        reachable
   * @return the local ip address, which is not a loopback address and is reachable
   */
  public static String getLocalIpAddress(int timeout) {
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
      if (address.isAnyLocalAddress() || address.isLoopbackAddress()
          || !address.isReachable(timeout) || !(address instanceof Inet4Address)) {
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
            if (!address.isLinkLocalAddress() && !address.isLoopbackAddress()
                && (address instanceof Inet4Address) && address.isReachable(timeout)) {
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
   * Replace and resolve the hostname in a given address or path string.
   *
   * @param path an address or path string, e.g., "hdfs://host:port/dir", "file:///dir", "/dir".
   * @return an address or path string with hostname resolved, or the original path intact if no
   *         hostname is embedded, or null if the given path is null or empty.
   * @throws UnknownHostException if the hostname cannot be resolved.
   */
  public static TachyonURI replaceHostName(TachyonURI path) throws UnknownHostException {
    if (path == null) {
      return null;
    }

    if (path.hasAuthority() && path.getPort() != -1) {
      String authority = resolveHostName(path.getHost());
      if (path.getPort() != -1) {
        authority += ":" + path.getPort();
      }
      return new TachyonURI(path.getScheme(), authority, path.getPath());
    }
    return path;
  }

  /**
   * Resolve a given hostname by a canonical hostname. When a hostname alias (e.g., those specified
   * in /etc/hosts) is given, the alias may not be resolvable on other hosts in a cluster unless the
   * same alias is defined there. In this situation, loadufs would break.
   *
   * @param hostname the input hostname, which could be an alias.
   * @return the canonical form of the hostname, or null if it is null or empty.
   * @throws UnknownHostException if the given hostname cannot be resolved.
   */
  public static String resolveHostName(String hostname) throws UnknownHostException {
    if (hostname == null || hostname.isEmpty()) {
      return null;
    }

    return InetAddress.getByName(hostname).getCanonicalHostName();
  }

  /**
   * Get FQDN(Full Qualified Domain Name) from representations of network address in Tachyon, except
   * String representation which should be handled by #resolveHostName(String hostname) which will
   * handle the situation where hostname is null.
   *
   * @param addr the input network address representation, can not be null
   * @return the resolved FQDN host name
   */
  public static String getFqdnHost(InetSocketAddress addr) {
    return addr.getAddress().getCanonicalHostName();
  }

  public static String getFqdnHost(NetAddress addr) throws UnknownHostException {
    return resolveHostName(addr.getMHost());
  }

  /**
   * Gets the port number on a given service type. If user defined port number is not explicitly
   * specified, Tachyon will select the default port number on the service.
   *
   * @param service Service type used to connect
   * @param conf Tachyon configuration used to look up the host resolution timeout
   * @return the service port number.
   */
  public static int getPort(ServiceType service, TachyonConf conf) {
    return conf.getInt(service.mPortKey, service.mDefaultPort);
  }

  /**
   * Gets the port for the underline socket. This function calls
   * {@link #getSocket(org.apache.thrift.transport.TServerSocket)}, so reflection will be
   * used to get the port.
   *
   * @see #getSocket(org.apache.thrift.transport.TServerSocket)
   */
  public static int getPort(TServerSocket thriftSocket) {
    return getSocket(thriftSocket).getLocalPort();
  }

  /**
   * Extracts the port from the thrift socket. As of thrift 0.9, the internal socket used is not
   * exposed in the API, so this function will use reflection to get access to it.
   *
   * @throws java.lang.RuntimeException if reflection calls fail
   */
  public static ServerSocket getSocket(final TServerSocket thriftSocket) {
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
   * Gets the Tachyon master address from the configuration
   *
   * @param conf the configuration of Tachyon
   * @return the InetSocketAddress of the master
   */
  public static InetSocketAddress getMasterAddress(TachyonConf conf) {
    String masterHostname =
        conf.get(Constants.MASTER_HOSTNAME, getLocalHostName(conf));
    // Cannot rely on tachyon-default.properties because GetMasterWorkerAddressTest will test with
    // fake conf
    int masterPort = conf.getInt(Constants.MASTER_PORT, Constants.DEFAULT_MASTER_PORT);
    return new InetSocketAddress(masterHostname, masterPort);
  }

  /**
   * Gets the {@link java.net.InetSocketAddress} of the local worker.
   *
   * Make sure there is a local worker before calling this method.
   *
   * @param conf the configuration of Tachyon
   * @return the worker's address
   */
  public static InetSocketAddress getLocalWorkerAddress(TachyonConf conf) {
    String workerHostname = getLocalHostName(conf);
    // Cannot rely on tachyon-default.properties because GetMasterWorkerAddressTest will test with
    // fake conf
    int workerPort = conf.getInt(Constants.WORKER_PORT, Constants.DEFAULT_WORKER_PORT);
    return new InetSocketAddress(workerHostname, workerPort);
  }

  /**
   * Parse InetSocketAddress from a String
   *
   * @param address
   * @return InetSocketAddress of the String
   * @throws IOException
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
}
