package tachyon.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class NetUtils {

  /*
   * Much More robust mechanism to get the hostName of the Machine of which a
   * server is running. Addresses bugs due to Java7 port on OS_X
   * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7180557
   * 
   * This method gets the network name of the machine we are running on. Returns
   * "UNKNOWN_LOCALHOST" in the unlikely case where the host name cannot be
   * found. Returns: String the name of the local host
   */
  public static String getLocalHostName() throws SocketException,
      UnknownHostException {
    try {
      final InetAddress addr = InetAddress.getLocalHost();
      return addr.getHostName();
    } catch (final UnknownHostException uhe) {
      final Enumeration<NetworkInterface> interfaces = NetworkInterface
          .getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        final NetworkInterface nic = interfaces.nextElement();
        final Enumeration<InetAddress> addresses = nic.getInetAddresses();
        while (addresses.hasMoreElements()) {
          final InetAddress address = addresses.nextElement();
          if (!address.isLoopbackAddress()) {
            final String hostname = address.getHostName();
            if (hostname != null) {
              return hostname;
            }
          }
        }
      }
      throw uhe;
    }
  }

  /*
   * Robust Mechanism to get the address of the Localhost. Addresses bugs due to
   * Java7 port on OS_X
   * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7180557
   * 
   * This method gets the network name of the machine we are running on. Returns
   * "UNKNOWN_LOCALHOST" in the unlikely case where the host name cannot be
   * found. Returns: String the name of the local host
   */
  public static String getLocalHostAddress() throws SocketException,
      UnknownHostException {
    try {
      final InetAddress addr = InetAddress.getLocalHost();
      return addr.getHostAddress();
    } catch (final UnknownHostException uhe) {
      final Enumeration<NetworkInterface> interfaces = NetworkInterface
          .getNetworkInterfaces();
      while (interfaces.hasMoreElements()) {
        final NetworkInterface nic = interfaces.nextElement();
        final Enumeration<InetAddress> addresses = nic.getInetAddresses();
        while (addresses.hasMoreElements()) {
          final InetAddress address = addresses.nextElement();
          if (!address.isLoopbackAddress()) {
            final String hostname = address.getHostAddress();
            if (hostname != null) {
              return hostname;
            }
          }
        }
      }
      throw uhe;
    }
  }

}
