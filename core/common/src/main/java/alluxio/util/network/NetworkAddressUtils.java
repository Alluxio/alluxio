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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GetServiceVersionPRequest;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.ServiceVersionClientServiceGrpc;
import alluxio.security.user.UserState;
import alluxio.util.CommonUtils;
import alluxio.util.OSUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Common network address related utilities shared by all components in Alluxio.
 */
@ThreadSafe
public final class NetworkAddressUtils {
  private static final Logger LOG = LoggerFactory.getLogger(NetworkAddressUtils.class);

  public static final String WILDCARD_ADDRESS = "0.0.0.0";
  public static final String UNKNOWN_HOSTNAME = "<UNKNOWN>";

  /**
   * Checks if the underlying OS is Windows.
   */
  public static final boolean WINDOWS = OSUtils.isWindows();

  private static String sLocalHost;
  private static String sLocalHostMetricName;
  private static String sLocalIP;

  private NetworkAddressUtils() {}

  /**
   * Different types of services that client uses to connect. These types also indicate the service
   * bind address.
   */
  public enum ServiceType {
    /**
     * FUSE web service (Jetty).
     */
    FUSE_WEB("Alluxio FUSE Web service", PropertyKey.FUSE_WEB_HOSTNAME,
        PropertyKey.FUSE_WEB_BIND_HOST, PropertyKey.FUSE_WEB_PORT),
    /**
     * Job master Raft service (Netty). The bind and connect hosts are the same because the
     * underlying Raft implementation doesn't differentiate between bind and connect hosts.
     */
    JOB_MASTER_RAFT("Alluxio Job Master Raft service", PropertyKey.JOB_MASTER_HOSTNAME,
        PropertyKey.JOB_MASTER_HOSTNAME, PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_PORT),

    /**
     * Master Raft service (Netty). The bind and connect hosts are the same because the
     * underlying Raft implementation doesn't differentiate between bind and connect hosts.
     */
    MASTER_RAFT("Alluxio Master Raft service", PropertyKey.MASTER_HOSTNAME,
        PropertyKey.MASTER_HOSTNAME, PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT),

    /**
     * Job master RPC service (gRPC).
     */
    JOB_MASTER_RPC("Alluxio Job Manager Master RPC service", PropertyKey.JOB_MASTER_HOSTNAME,
        PropertyKey.JOB_MASTER_BIND_HOST, PropertyKey.JOB_MASTER_RPC_PORT),

    /**
     * Job master web service (Jetty).
     */
    JOB_MASTER_WEB("Alluxio Job Manager Master Web service", PropertyKey.JOB_MASTER_WEB_HOSTNAME,
        PropertyKey.JOB_MASTER_WEB_BIND_HOST, PropertyKey.JOB_MASTER_WEB_PORT),

    /**
     * Job worker RPC service (gRPC).
     */
    JOB_WORKER_RPC("Alluxio Job Manager Worker RPC service", PropertyKey.JOB_WORKER_HOSTNAME,
        PropertyKey.JOB_WORKER_BIND_HOST, PropertyKey.JOB_WORKER_RPC_PORT),

    /**
     * Job master web service (Jetty).
     */
    JOB_WORKER_WEB("Alluxio Job Manager Worker Web service", PropertyKey.WORKER_WEB_HOSTNAME,
        PropertyKey.JOB_WORKER_WEB_BIND_HOST, PropertyKey.JOB_WORKER_WEB_PORT),

    /**
     * Master RPC service (gRPC).
     */
    MASTER_RPC("Alluxio Master RPC service", PropertyKey.MASTER_HOSTNAME,
        PropertyKey.MASTER_BIND_HOST, PropertyKey.MASTER_RPC_PORT),

    /**
     * Master web service (Jetty).
     */
    MASTER_WEB("Alluxio Master Web service", PropertyKey.MASTER_WEB_HOSTNAME,
        PropertyKey.MASTER_WEB_BIND_HOST, PropertyKey.MASTER_WEB_PORT),

    /**
     * Worker RPC service (gRPC).
     */
    WORKER_RPC("Alluxio Worker RPC service", PropertyKey.WORKER_HOSTNAME,
        PropertyKey.WORKER_BIND_HOST, PropertyKey.WORKER_RPC_PORT),

    /**
     * Worker web service (Jetty).
     */
    WORKER_WEB("Alluxio Worker Web service", PropertyKey.WORKER_WEB_HOSTNAME,
        PropertyKey.WORKER_WEB_BIND_HOST, PropertyKey.WORKER_WEB_PORT),

    /**
     * Proxy web service (Jetty).
     */
    PROXY_WEB("Alluxio Proxy Web service", PropertyKey.PROXY_WEB_HOSTNAME,
        PropertyKey.PROXY_WEB_BIND_HOST, PropertyKey.PROXY_WEB_PORT),
    ;

    // service name
    private final String mServiceName;

    // the key of connect hostname
    private final PropertyKey mHostNameKey;

    // the key of bind hostname
    private final PropertyKey mBindHostKey;

    // the key of service port
    private final PropertyKey mPortKey;

    ServiceType(String serviceName, PropertyKey hostNameKey, PropertyKey bindHostKey,
        PropertyKey portKey) {
      mServiceName = serviceName;
      mHostNameKey = hostNameKey;
      mBindHostKey = bindHostKey;
      mPortKey = portKey;
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
    public PropertyKey getHostNameKey() {
      return mHostNameKey;
    }

    /**
     * Gets the key of bind hostname.
     *
     * @return key of bind hostname
     */
    public PropertyKey getBindHostKey() {
      return mBindHostKey;
    }

    /**
     * Gets the key of service port.
     *
     * @return key of service port
     */
    public PropertyKey getPortKey() {
      return mPortKey;
    }

    /**
     * Gets the default port number on service.
     *
     * @return default port
     */
    public int getDefaultPort() {
      return Integer.parseInt(mPortKey.getDefaultValue());
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
   * @param conf the configuration to use for looking up the connect address
   * @return the service address that a client (typically outside the service machine) uses to
   *         communicate with service.
   */
  public static InetSocketAddress getConnectAddress(ServiceType service,
      AlluxioConfiguration conf) {
    return InetSocketAddress.createUnresolved(getConnectHost(service, conf),
        getPort(service, conf));
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
   * <th>Enable Network IP Address Used</th>
   * <th>Returned Connect Host</th>
   * </tr>
   * </thead>
   * <tbody>
   * <tr>
   * <td>hostname</td>
   * <td>hostname</td>
   * <td>true/false</td>
   * <td>hostname</td>
   * </tr>
   * <tr>
   * <td>not defined</td>
   * <td>hostname</td>
   * <td>true/false</td>
   * <td>hostname</td>
   * </tr>
   * <tr>
   * <td>hostname</td>
   * <td>0.0.0.0 or not defined</td>
   * <td>true/false</td>
   * <td>hostname</td>
   * </tr>
   * <tr>
   * <td>not defined</td>
   * <td>0.0.0.0 or not defined</td>
   * <td>false</td>
   * <td>local hostname</td>
   * </tr>
   * <tr>
   * <td>not defined</td>
   * <td>0.0.0.0 or not defined</td>
   * <td>true</td>
   * <td>local IP address</td>
   * </tr>
   * </tbody>
   * </table>
   *
   * @param service Service type used to connect
   * @param conf configuration
   * @return the externally resolvable hostname that the client can use to communicate with the
   *         service.
   */
  public static String getConnectHost(ServiceType service, AlluxioConfiguration conf) {
    if (conf.isSet(service.mHostNameKey)) {
      String connectHost = conf.get(service.mHostNameKey);
      if (!connectHost.isEmpty() && !connectHost.equals(WILDCARD_ADDRESS)) {
        return connectHost;
      }
    }
    if (conf.isSet(service.mBindHostKey)) {
      String bindHost = conf.get(service.mBindHostKey);
      if (!bindHost.isEmpty() && !bindHost.equals(WILDCARD_ADDRESS)) {
        return bindHost;
      }
    }
    if (conf.getBoolean(PropertyKey.NETWORK_IP_ADDRESS_USED)) {
      return getLocalIpAddress((int) conf.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    }
    return getLocalHostName((int) conf.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
  }

  /**
   * Gets the port number on a given service type. If user defined port number is not explicitly
   * specified, Alluxio will use the default service port.
   *
   * @param service Service type used to connect
   * @param conf Alluxio configuration
   * @return the service port number
   */
  public static int getPort(ServiceType service, AlluxioConfiguration conf) {
    return conf.getInt(service.mPortKey);
  }

  /**
   * Helper method to get the bind hostname for a given service.
   *
   * @param service the service name
   * @param conf Alluxio configuration
   * @return the InetSocketAddress the service will bind to
   */
  public static InetSocketAddress getBindAddress(ServiceType service, AlluxioConfiguration conf) {
    int port = getPort(service, conf);
    assertValidPort(port);
    return new InetSocketAddress(getBindHost(service, conf), getPort(service, conf));
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
   * @param service the service name
   * @param conf Alluxio configuration
   * @return the bind hostname
   */
  public static String getBindHost(ServiceType service, AlluxioConfiguration conf) {
    if (conf.isSet(service.mBindHostKey) && !conf.get(service.mBindHostKey)
        .isEmpty()) {
      return conf.get(service.mBindHostKey);
    } else {
      return getLocalHostName((int) conf.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    }
  }

  /**
   * Gets the local hostname to be used by the client. If this isn't configured, a
   * non-loopback local hostname will be looked up.
   *
   * @param conf Alluxio configuration
   * @return the local hostname for the client
   */
  public static String getClientHostName(AlluxioConfiguration conf) {
    if (conf.isSet(PropertyKey.USER_HOSTNAME)) {
      return conf.get(PropertyKey.USER_HOSTNAME);
    }
    if (conf.isSet(PropertyKey.LOCALITY_TIER_NODE)) {
      return conf.get(PropertyKey.LOCALITY_TIER_NODE);
    }
    return getLocalHostName((int) conf.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
  }

  /**
   * Gets a local node name from configuration if it is available, falling back on localhost lookup.
   *
   * @param conf Alluxio configuration
   * @return the local node name
   */
  public static String getLocalNodeName(AlluxioConfiguration conf) {
    switch (CommonUtils.PROCESS_TYPE.get()) {
      case JOB_MASTER:
        if (conf.isSet(PropertyKey.JOB_MASTER_HOSTNAME)) {
          return conf.get(PropertyKey.JOB_MASTER_HOSTNAME);
        }
        break;
      case JOB_WORKER:
        if (conf.isSet(PropertyKey.JOB_WORKER_HOSTNAME)) {
          return conf.get(PropertyKey.JOB_WORKER_HOSTNAME);
        }
        break;
      case CLIENT:
        if (conf.isSet(PropertyKey.USER_HOSTNAME)) {
          return conf.get(PropertyKey.USER_HOSTNAME);
        }
        break;
      case MASTER:
        if (conf.isSet(PropertyKey.MASTER_HOSTNAME)) {
          return conf.get(PropertyKey.MASTER_HOSTNAME);
        }
        break;
      case WORKER:
        if (conf.isSet(PropertyKey.WORKER_HOSTNAME)) {
          return conf.get(PropertyKey.WORKER_HOSTNAME);
        }
        break;
      default:
        break;
    }
    if (conf.isSet(PropertyKey.LOCALITY_TIER_NODE)) {
      return conf.get(PropertyKey.LOCALITY_TIER_NODE);
    }
    return getLocalHostName((int) conf.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
  }

  /**
   * Gets a local host name for the host this JVM is running on.
   *
   * @param timeoutMs Timeout in milliseconds to use for checking that a possible local host is
   *        reachable
   * @return the local host name, which is not based on a loopback ip address
   */
  public static synchronized String getLocalHostName(int timeoutMs) {
    if (sLocalHost != null) {
      return sLocalHost;
    }

    try {
      sLocalHost = InetAddress.getByName(getLocalIpAddress(timeoutMs)).getCanonicalHostName();
      return sLocalHost;
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets a local hostname for the host this JVM is running on with '.' replaced with '_' for
   * metrics usage.
   *
   * @param timeoutMs Timeout in milliseconds to use for checking that a possible local host is
   * reachable
   * @return the metrics system friendly local host name
   */
  public static synchronized String getLocalHostMetricName(int timeoutMs) {
    if (sLocalHostMetricName != null) {
      return sLocalHostMetricName;
    }
    sLocalHostMetricName = getLocalHostName(timeoutMs).replace('.', '_');
    return sLocalHostMetricName;
  }

  /**
   * Gets a local IP address for the host this JVM is running on.
   *
   * @param timeoutMs Timeout in milliseconds to use for checking that a possible local IP is
   *        reachable
   * @return the local ip address, which is not a loopback address and is reachable
   */
  public static synchronized String getLocalIpAddress(int timeoutMs) {
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
      if (!isValidAddress(address, timeoutMs)) {
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
            if (isValidAddress(address, timeoutMs)) {
              sLocalIP = address.getHostAddress();
              return sLocalIP;
            }
          }
        }

        LOG.warn("Your hostname, {} resolves to a loopback/non-reachable address: {}, "
                  +  "but we couldn't find any external IP address!",
                  InetAddress.getLocalHost().getHostName(), address.getHostAddress());
      }

      sLocalIP = address.getHostAddress();
      return sLocalIP;
    } catch (IOException e) {
      throw new RuntimeException(e);
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
   * @param timeoutMs Timeout in milliseconds to use for checking that a possible local IP is
   *        reachable
   * @return a {@code boolean} indicating if the given address is externally resolvable address
   */
  private static boolean isValidAddress(InetAddress address, int timeoutMs) throws IOException {
    return !address.isAnyLocalAddress() && !address.isLinkLocalAddress()
        && !address.isLoopbackAddress() && address.isReachable(timeoutMs)
        && (address instanceof Inet4Address);
  }

  /**
   * Resolves a given hostname by a canonical hostname. When a hostname alias (e.g., those specified
   * in /etc/hosts) is given, the alias may not be resolvable on other hosts in a cluster unless the
   * same alias is defined there.
   *
   * @param hostname the input hostname, which could be an alias
   * @return the canonical form of the hostname, or null if it is null or empty
   * @throws UnknownHostException if the given hostname cannot be resolved
   */
  @Nullable
  public static String resolveHostName(String hostname) throws UnknownHostException {
    if (hostname == null || hostname.isEmpty()) {
      return null;
    }

    return InetAddress.getByName(hostname).getCanonicalHostName();
  }

  /**
   * Resolves a given hostname IP address.
   *
   * @param hostname the input hostname, which could be an alias
   * @return the hostname IP address
   * @throws UnknownHostException if the given hostname cannot be resolved
   */
  public static String resolveIpAddress(String hostname) throws UnknownHostException {
    Preconditions.checkNotNull(hostname, "hostname");
    Preconditions.checkArgument(!hostname.isEmpty(),
            "Cannot resolve IP address for empty hostname");
    return InetAddress.getByName(hostname).getHostAddress();
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
   * Parses {@link InetSocketAddress} from a String.
   *
   * @param address socket address to parse
   * @return InetSocketAddress of the String
   */
  @Nullable
  public static InetSocketAddress parseInetSocketAddress(String address) throws IOException {
    if (address == null) {
      return null;
    }
    String[] strArr = address.split(":");
    if (strArr.length != 2) {
      throw new IOException("Invalid InetSocketAddress " + address);
    }
    return InetSocketAddress.createUnresolved(strArr[0], Integer.parseInt(strArr[1]));
  }

  /**
   * Extracts rpcPort InetSocketAddress from Alluxio representation of network address.
   *
   * @param netAddress the input network address representation
   * @return InetSocketAddress
   */
  public static InetSocketAddress getRpcPortSocketAddress(WorkerNetAddress netAddress) {
    String host = netAddress.getHost();
    int port = netAddress.getRpcPort();
    return new InetSocketAddress(host, port);
  }

  /**
   * Extracts dataPort socket address from Alluxio representation of network address.
   *
   * @param netAddress the input network address representation
   * @param conf Alluxio configuration
   * @return the socket address
   */
  public static SocketAddress getDataPortSocketAddress(WorkerNetAddress netAddress,
      AlluxioConfiguration conf) {
    SocketAddress address;
    if (NettyUtils.isDomainSocketAccessible(netAddress, conf)) {
      address = new DomainSocketAddress(netAddress.getDomainSocketPath());
    } else {
      String host = netAddress.getHost();
      // issues#11172: If the worker is in a container, use the container hostname
      // to establish the connection.
      if (!netAddress.getContainerHost().equals("")) {
        LOG.debug("Worker is in a container. Use container host {} instead of physical host {}",
                netAddress.getContainerHost(), host);
        host = netAddress.getContainerHost();
      }

      int port = netAddress.getDataPort();
      address = new InetSocketAddress(host, port);
    }
    return address;
  }

  /**
   * Test if the input address is serving an Alluxio service. This method make use of the
   * gRPC protocol for performing service communication.
   * This methods throws UnauthenticatedException if the user is not authenticated,
   * StatusRuntimeException If the host not reachable or does not serve the given service.
   *
   * @param address the network address to ping
   * @param serviceType the Alluxio service type
   * @param conf Alluxio configuration
   * @param userState the UserState
   */
  public static void pingService(InetSocketAddress address, alluxio.grpc.ServiceType serviceType,
      AlluxioConfiguration conf, UserState userState)
      throws AlluxioStatusException {
    Preconditions.checkNotNull(address, "address");
    Preconditions.checkNotNull(serviceType, "serviceType");
    GrpcChannel channel = GrpcChannelBuilder.newBuilder(GrpcServerAddress.create(address), conf)
        .setClientType("PingService").disableAuthentication().setSubject(userState.getSubject())
        .build();
    try {
      ServiceVersionClientServiceGrpc.ServiceVersionClientServiceBlockingStub versionClient =
          ServiceVersionClientServiceGrpc.newBlockingStub(channel)
              .withDeadlineAfter(conf.getMs(PropertyKey.USER_MASTER_POLLING_TIMEOUT),
                  TimeUnit.MILLISECONDS);
      versionClient.getServiceVersion(
          GetServiceVersionPRequest.newBuilder().setServiceType(serviceType).build());
    } catch (Throwable t) {
      throw AlluxioStatusException.fromThrowable(t);
    } finally {
      channel.shutdown();
    }
  }

  /**
   * @param clusterAddresses addresses of all nodes in the Raft cluster
   * @param conf Alluxio configuration
   * @return true if the cluster addresses contain the local IP, false otherwise
   */
  public static boolean containsLocalIp(List<InetSocketAddress> clusterAddresses,
      AlluxioConfiguration conf) {
    String localAddressIp = getLocalIpAddress((int) conf.getMs(PropertyKey
        .NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    for (InetSocketAddress addr : clusterAddresses) {
      String clusterNodeIp;
      try {
        clusterNodeIp = InetAddress.getByName(addr.getHostName()).getHostAddress();
        if (clusterNodeIp.equals(localAddressIp)) {
          return true;
        }
      } catch (UnknownHostException e) {
        LOG.error("Get raft cluster node ip by hostname({}) failed",
            addr.getHostName(), e);
      }
    }
    return false;
  }
}
