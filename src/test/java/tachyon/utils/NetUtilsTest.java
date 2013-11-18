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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

/*
 * Since this Class is basically to adapt to JDK Bugs in Mac OS X
 * the expected results vary based on machine on which this program is run and how java 
 * behaves in that environment. These tests capture the expected and
 * checks if the NetUtils returns the same
 * 
 * Also further if the returned value is not unknownLocalHost Further, Reachability Tests
 * are performed for known Hosts
 */
public class NetUtilsTest {
  private static final String UNKNOWN_LOCALHOST = "unknownLocalHost";
  private static final String UNKNOWN_LOCALHOSTADDRESS = "unknownLocalHostAddress";

  // This contains the expected hostname for the current Machine;
  private static String expectedHostName;
  // This contains the expected hostAddress for the current Machine;
  private static String expectedHostAddress;

  @BeforeClass
  public static void setUp() {
    try {
      expectedHostName = InetAddress.getLocalHost().getHostName();
      expectedHostAddress = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      try {
        final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        // get the first resolvable host name
        while (interfaces.hasMoreElements()) {
          final NetworkInterface nic = interfaces.nextElement();
          final Enumeration<InetAddress> addresses = nic.getInetAddresses();
          while (addresses.hasMoreElements()) {
            final InetAddress address = addresses.nextElement();
            if (!address.isLoopbackAddress()) {
              final String hostname = address.getHostName();
              if (hostname != null) {
                expectedHostName = hostname;
                expectedHostAddress = address.getHostAddress();
                break;
              }
            }
          }
          if (expectedHostName != null) {
            break;
          }
        }
      } catch (final SocketException se) {
        expectedHostName = UNKNOWN_LOCALHOST;
        expectedHostAddress = UNKNOWN_LOCALHOSTADDRESS;
      }
      if (expectedHostName == null) {
        expectedHostName = UNKNOWN_LOCALHOST;
        expectedHostAddress = UNKNOWN_LOCALHOSTADDRESS;
      }
    }
  }

  @Test
  public void testHostNameEquality() {
    Assert.assertEquals(expectedHostName, NetUtils.getLocalHostName());

  }

  @Test
  public void testHostNameReachability() {
    // Do the Reachability if we have a known address
    if (!UNKNOWN_LOCALHOST.equals(NetUtils.getLocalHostName())) {
      try {
        Assert.assertTrue(InetAddress.getByName(NetUtils.getLocalHostName()).isReachable(300));
      } catch (IOException e) {
        fail(expectedHostName + " is not reachable");
      }
    }

  }

  @Test
  public void testHostAddressEquality() {
    Assert.assertEquals(expectedHostAddress, NetUtils.getLocalHostAddress());
  }

  @Test
  public void testHostAddressReachability() {
    // Do the Reachability if we have a known address
    if (!UNKNOWN_LOCALHOSTADDRESS.equals(NetUtils.getLocalHostAddress())) {
      try {
        Assert.assertTrue(InetAddress.getByName(NetUtils.getLocalHostAddress()).isReachable(300));
      } catch (IOException e) {
        fail(expectedHostAddress + " is not reachable");
      }
    }

  }
}
