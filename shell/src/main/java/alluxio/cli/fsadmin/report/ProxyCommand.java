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

import alluxio.client.meta.MetaMasterClient;
import alluxio.grpc.BuildVersion;
import alluxio.grpc.NetAddress;
import alluxio.grpc.ProxyStatus;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.List;
import java.util.Locale;

/**
 * Prints information about proxy instances in the cluster.
 */
public class ProxyCommand {
  private final MetaMasterClient mMetaMasterClient;
  private final PrintStream mPrintStream;

  public static final DateTimeFormatter DATETIME_FORMAT =
          DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).ofPattern("yyyyMMdd-HHmmss")
                  .withLocale(Locale.getDefault()).withZone(ZoneId.systemDefault());

  /**
   * Creates a new instance of {@link ProxyCommand}.
   *
   * @param metaMasterClient the client to talk to the master with
   * @param printStream the stream to print to
   */
  public ProxyCommand(MetaMasterClient metaMasterClient, PrintStream printStream) {
    mMetaMasterClient = metaMasterClient;
    mPrintStream = printStream;
  }

  /**
   * Runs a proxy report command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    String[] header = new String[]{"Address", "State", "Start Time", "Last Heartbeat Time",
        "Version", "Revision"};

    try {
      List<ProxyStatus> allProxyStatus = mMetaMasterClient.listProxyStatus();
      int liveCount = 0;
      int lostCount = 0;
      int maxAddressLength = 24;
      for (ProxyStatus proxyStatus : allProxyStatus) {
        String state = proxyStatus.getState();
        if (state.equals("ACTIVE")) {
          liveCount++;
        } else if (state.equals("LOST")) {
          lostCount++;
        }
        NetAddress address = proxyStatus.getAddress();
        String addressStr = address.getHost() + ":" + address.getRpcPort();
        if (maxAddressLength < addressStr.length()) {
          maxAddressLength = addressStr.length();
        }
      }
      mPrintStream.printf("%s Proxy instances in the cluster, %s serving and %s lost%n%n",
              liveCount + lostCount, liveCount, lostCount);

      String format = "%-" + maxAddressLength + "s %-8s %-16s %-20s %-32s %-8s%n";
      mPrintStream.printf(format, header);
      for (ProxyStatus proxyStatus : allProxyStatus) {
        NetAddress address = proxyStatus.getAddress();
        BuildVersion version = proxyStatus.getVersion();
        mPrintStream.printf(format,
                address.getHost() + ":" + address.getRpcPort(),
                proxyStatus.getState(),
                DATETIME_FORMAT.format(Instant.ofEpochMilli(proxyStatus.getStartTime())),
                DATETIME_FORMAT.format(Instant.ofEpochMilli(proxyStatus.getLastHeartbeatTime())),
                version.getVersion(), version.getRevision());
      }
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    }
  }
}
