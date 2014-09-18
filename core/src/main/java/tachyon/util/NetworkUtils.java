package tachyon.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Enumeration;

import org.apache.thrift.transport.TNonblockingServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.thrift.NetAddress;

/**
 * Common network utilities shared by all components in Tachyon.
 */
public final class NetworkUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private NetworkUtils() {}

  /**
   * @return the local host name, which is not based on a loopback ip address.
   */
  public static String getLocalHostName() {
    try {
      return InetAddress.getByName(getLocalIpAddress()).getCanonicalHostName();
    } catch (UnknownHostException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * @return the local ip address, which is not a loopback address.
   */
  public static String getLocalIpAddress() {
    try {
      InetAddress address = InetAddress.getLocalHost();
      LOG.debug("address: {} isLoopbackAddress: {}, with host {} {}", address,
          address.isLoopbackAddress(), address.getHostAddress(), address.getHostName());
      if (address.isLoopbackAddress()) {
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
          NetworkInterface ni = networkInterfaces.nextElement();
          Enumeration<InetAddress> addresses = ni.getInetAddresses();
          while (addresses.hasMoreElements()) {
            address = addresses.nextElement();

            if (!address.isLinkLocalAddress() && !address.isLoopbackAddress()
                && (address instanceof Inet4Address)) {
              return address.getHostAddress();
            }
          }
        }

        LOG.warn("Your hostname, " + InetAddress.getLocalHost().getHostName() + " resolves to"
            + " a loopback address: " + address.getHostAddress() + ", but we couldn't find any"
            + " external IP address!");
      }

      return address.getHostAddress();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Replace and resolve the hostname in a given address or path string.
   * 
   * @param addr an address or path string, e.g., "hdfs://host:port/dir", "file:///dir", "/dir".
   * @return an address or path string with hostname resolved, or the original path intact if no
   *         hostname is embedded, or null if the given path is null or empty.
   * @throws UnknownHostException if the hostname cannot be resolved.
   */
  public static String replaceHostName(String addr) throws UnknownHostException {
    if (addr == null || addr.isEmpty()) {
      return null;
    }

    if (addr.contains("://")) {
      int idx = addr.indexOf("://");
      String prefix = addr.substring(0, idx + 3);
      String rest = addr.substring(idx + 3);
      if (rest.contains(":")) {
        // case host:port/dir or host:port or host:port/
        int idx2 = rest.indexOf(":");
        String hostname = rest.substring(0, idx2);
        hostname = resolveHostName(hostname);
        String suffix = rest.substring(idx2);
        return prefix + hostname + suffix;
      } else if (rest.contains(Constants.PATH_SEPARATOR)) {
        // case host/dir or /dir or host/
        int idx2 = rest.indexOf(Constants.PATH_SEPARATOR);
        if (idx2 > 0) {
          String hostname = rest.substring(0, idx2);
          hostname = resolveHostName(hostname);
          String suffix = rest.substring(idx2);
          return prefix + hostname + suffix;
        }
      } else {
        // case host is rest of the path
        return prefix + resolveHostName(rest);
      }
    }

    return addr;
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
   * Gets the port for the underline socket. This function calls
   * {@link #getSocket(org.apache.thrift.transport.TNonblockingServerSocket)}, so reflection will be
   * used to get the port.
   * 
   * @see #getSocket(org.apache.thrift.transport.TNonblockingServerSocket)
   */
  public static int getPort(TNonblockingServerSocket thriftSocket) {
    return getSocket(thriftSocket).getLocalPort();
  }

  /**
   * Extracts the port from the thrift socket. As of thrift 0.9, the internal socket used is not
   * exposed in the API, so this function will use reflection to get access to it.
   * 
   * @throws java.lang.RuntimeException if reflection calls fail
   */
  public static ServerSocket getSocket(final TNonblockingServerSocket thriftSocket) {
    try {
      Field field = TNonblockingServerSocket.class.getDeclaredField("serverSocket_");
      field.setAccessible(true);
      return (ServerSocket) field.get(thriftSocket);
    } catch (NoSuchFieldException e) {
      throw Throwables.propagate(e);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }
}
