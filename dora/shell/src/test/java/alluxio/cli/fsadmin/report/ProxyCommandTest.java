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

package alluxio.cli.fsadmin.report;

import alluxio.client.meta.RetryHandlingMetaMasterClient;
import alluxio.grpc.BuildVersion;
import alluxio.grpc.NetAddress;
import alluxio.grpc.ProxyStatus;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProxyCommandTest {
  private RetryHandlingMetaMasterClient mMetaMasterClient;

  @Before
  public void prepareDependencies() throws IOException {
    mMetaMasterClient = Mockito.mock(RetryHandlingMetaMasterClient.class);
  }

  @Test
  public void listProxyInstances() throws IOException {
    List<ProxyStatus> longInfoList = prepareInfoList();
    Mockito.when(mMetaMasterClient.listProxyStatus())
            .thenReturn(longInfoList);

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
         PrintStream printStream = new PrintStream(outputStream, true, "utf-8")) {
      ProxyCommand proxyCommand = new ProxyCommand(mMetaMasterClient, printStream);
      proxyCommand.run();
      String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
      // CHECKSTYLE.OFF: LineLengthExceed - Much more readable
      List<String> expectedOutput = Arrays.asList("2 Proxy instances in the cluster, 1 serving and 1 lost",
          "",
          "Address                  State    Start Time       Last Heartbeat Time  Version                          Revision",
          "proxy-0:12345            ACTIVE   20230421-182944  20230421-183005      1.0                              abc     ",
          "proxy-1:23456            LOST     20230421-182707  20230421-190507      1.1                              abc     ");
      // CHECKSTYLE.ON: LineLengthExceed
      List<String> testOutput = Arrays.asList(output.split("\n"));
      Assert.assertThat(testOutput,
              IsIterableContainingInOrder.contains(expectedOutput.toArray()));
    }
  }

  @Test
  public void listProxyInstancesLongName() throws IOException {
    List<ProxyStatus> longInfoList = prepareInfoListLongName();
    Mockito.when(mMetaMasterClient.listProxyStatus())
            .thenReturn(longInfoList);

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
         PrintStream printStream = new PrintStream(outputStream, true, "utf-8")) {
      ProxyCommand proxyCommand = new ProxyCommand(mMetaMasterClient, printStream);
      proxyCommand.run();
      String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
      // CHECKSTYLE.OFF: LineLengthExceed - Much more readable
      List<String> expectedOutput = Arrays.asList("2 Proxy instances in the cluster, 1 serving and 1 lost",
          "",
          "Address                                       State    Start Time       Last Heartbeat Time  Version                          Revision",
          "datacenter-namespace-department-proxy-0:12345 ACTIVE   20230421-182944  20230421-183005      1.0                              abc     ",
          "datacenter-namespace-department-proxy-1:23456 LOST     20230421-182707  20230421-190507      1.1                              abc     ");
      // CHECKSTYLE.ON: LineLengthExceed
      List<String> testOutput = Arrays.asList(output.split("\n"));
      Assert.assertThat(testOutput,
          IsIterableContainingInOrder.contains(expectedOutput.toArray()));
    }
  }

  /**
   * @return long worker info list to test
   */
  private List<ProxyStatus> prepareInfoList() {
    List<ProxyStatus> infoList = new ArrayList<>();

    long proxy0startTime = Instant.parse("2023-04-21T18:29:44").getMillis();
    long proxy0HeartbeatTime = Instant.parse("2023-04-21T18:30:05").getMillis();
    NetAddress address0 = NetAddress.newBuilder().setHost("proxy-0").setRpcPort(12345).build();
    BuildVersion version0 = BuildVersion.newBuilder().setVersion("1.0").setRevision("abc").build();
    ProxyStatus proxy0 = ProxyStatus.newBuilder()
            .setAddress(address0).setState("ACTIVE")
            .setStartTime(proxy0startTime)
            .setLastHeartbeatTime(proxy0HeartbeatTime)
            .setVersion(version0).build();
    infoList.add(proxy0);

    long proxy1startTime = Instant.parse("2023-04-21T18:27:07").getMillis();
    long proxy1HeartbeatTime = Instant.parse("2023-04-21T19:05:07").getMillis();
    NetAddress address1 = NetAddress.newBuilder().setHost("proxy-1").setRpcPort(23456).build();
    BuildVersion version1 = BuildVersion.newBuilder().setVersion("1.1").setRevision("abc").build();
    ProxyStatus proxy1 = ProxyStatus.newBuilder()
            .setAddress(address1).setState("LOST")
            .setStartTime(proxy1startTime)
            .setLastHeartbeatTime(proxy1HeartbeatTime)
            .setVersion(version1).build();
    infoList.add(proxy1);

    return infoList;
  }

  private List<ProxyStatus> prepareInfoListLongName() {
    List<ProxyStatus> infoList = new ArrayList<>();

    long proxy0startTime = Instant.parse("2023-04-21T18:29:44").getMillis();
    long proxy0HeartbeatTime = Instant.parse("2023-04-21T18:30:05").getMillis();
    NetAddress address0 = NetAddress.newBuilder()
        .setHost("datacenter-namespace-department-proxy-0").setRpcPort(12345).build();
    BuildVersion version0 = BuildVersion.newBuilder().setVersion("1.0").setRevision("abc").build();
    ProxyStatus proxy0 = ProxyStatus.newBuilder().setAddress(address0).setState("ACTIVE")
        .setStartTime(proxy0startTime).setLastHeartbeatTime(proxy0HeartbeatTime)
        .setVersion(version0).build();
    infoList.add(proxy0);

    long proxy1startTime = Instant.parse("2023-04-21T18:27:07").getMillis();
    long proxy1HeartbeatTime = Instant.parse("2023-04-21T19:05:07").getMillis();
    NetAddress address1 = NetAddress.newBuilder()
        .setHost("datacenter-namespace-department-proxy-1").setRpcPort(23456).build();
    BuildVersion version1 = BuildVersion.newBuilder().setVersion("1.1").setRevision("abc").build();
    ProxyStatus proxy1 = ProxyStatus.newBuilder().setAddress(address1).setState("LOST")
        .setStartTime(proxy1startTime).setLastHeartbeatTime(proxy1HeartbeatTime)
        .setVersion(version1).build();
    infoList.add(proxy1);

    return infoList;
  }
}

