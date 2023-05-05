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

package alluxio.grpc;

import static alluxio.grpc.GrpcUtils.netAddressToSocketAddress;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

public class GrpcUtilsTest {

  @Test
  public void netAddressTest() throws Exception {
    List<NetAddress> addressList = Collections.singletonList(
        NetAddress.newBuilder().setHost("localhost").setRpcPort(1).build());
    InetSocketAddress[] inetSocketAddressList = netAddressToSocketAddress(addressList);
    Assert.assertEquals(1, inetSocketAddressList.length);
    Assert.assertEquals("localhost", inetSocketAddressList[0].getHostName());
    Assert.assertEquals(1, inetSocketAddressList[0].getPort());
  }
}
