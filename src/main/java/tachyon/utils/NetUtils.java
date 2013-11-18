/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

import org.apache.log4j.Logger;

public class NetUtils {
  private static final Logger LOGGER = Logger.getLogger(NetUtils.class.getName());
  private static final String UNKNOWN_LOCALHOST = "unknownLocalHost";
  private static final String UNKNOWN_LOCALHOSTADDRESS = "unknownLocalHostAddress";

  /*
   * Much More robust mechanism to get the hostName of the Machine of which a
   * server is running. Addresses bugs due to Java7 port on OS_X
   * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7180557
   * 
   * This method gets the network name of the machine we are running on. Returns
   * "UNKNOWN_LOCALHOST" in the unlikely case where the host name cannot be
   * found. Returns: String the name of the local host
   */
  public static String getLocalHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (final UnknownHostException uhe) {
      try {
        final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
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
      } catch (final SocketException se) {
        LOGGER.error("Could not determine local host name", uhe);
        return UNKNOWN_LOCALHOST;
      } finally {

      }
      LOGGER.error("Could not determine local host name", uhe);
      return UNKNOWN_LOCALHOST;
    }
  }

  /*
   * Robust Mechanism to get the address of the Localhost. Addresses bugs due to
   * Java7 port on OS_X
   * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7180557
   * 
   * This method gets the network name of the machine we are running on. Returns
   * "UNKNOWN_LOCALHOSTADDRESS" in the unlikely case where the host name cannot
   * be found. Returns: String the name of the local host
   */
  public static String getLocalHostAddress() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (final UnknownHostException uhe) {
      try {
        final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
          final NetworkInterface nic = interfaces.nextElement();
          final Enumeration<InetAddress> addresses = nic.getInetAddresses();
          while (addresses.hasMoreElements()) {
            final InetAddress address = addresses.nextElement();
            if (!address.isLoopbackAddress()) {
              final String hostaddress = address.getHostAddress();
              if (hostaddress != null) {
                return hostaddress;
              }
            }
          }
        }
      } catch (final SocketException se) {
        LOGGER.error("Could not determine local host address", uhe);
        return UNKNOWN_LOCALHOSTADDRESS;
      }
      LOGGER.error("Could not determine local host address", uhe);
      return UNKNOWN_LOCALHOSTADDRESS;
    }
  }

}
