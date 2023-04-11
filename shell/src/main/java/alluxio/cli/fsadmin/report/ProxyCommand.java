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

import alluxio.client.job.JobMasterClient;
import alluxio.client.meta.MetaMasterClient;
import alluxio.grpc.NetAddress;
import alluxio.grpc.ProxyStatus;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.job.wire.StatusSummary;
import alluxio.util.CommonUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * Prints job service metric information.
 */
public class ProxyCommand {

  private final MetaMasterClient mMetaMasterClient;
  private final PrintStream mPrintStream;

  public static final DateTimeFormatter DATETIME_FORMAT =
          DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).ofPattern("yyyyMMdd-HHmmss")
                  .withLocale(Locale.getDefault()).withZone(ZoneId.systemDefault());
  /**
   * Creates a new instance of {@link JobServiceMetricsCommand}.
   *
   * @param JobMasterClient client to connect to job master client
   * @param printStream stream to print job services metrics information to
   * @param dateFormatPattern the pattern to follow when printing the date
   */
  public ProxyCommand(MetaMasterClient metaMasterClient, PrintStream printStream) {
    mMetaMasterClient = metaMasterClient;
    mPrintStream = printStream;
  }

  /**
   * Runs a job services report metrics command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    List<ProxyStatus> allProxyStatus = mMetaMasterClient.listProxyStatus();

    // TODO(jiacheng): print a table
    for (ProxyStatus proxyStatus : allProxyStatus) {
      NetAddress address = proxyStatus.getAddress();
      mPrintStream.printf("Proxy Address: %-24s  ", address.getHost() + ":" + address.getRpcPort());
      mPrintStream.printf("Proxy State: %-8s  ", proxyStatus.getState());
      mPrintStream.printf("Proxy Start Time: %-16s",
          DATETIME_FORMAT.format(Instant.ofEpochMilli(proxyStatus.getStartTime())));
      mPrintStream.printf("Proxy Last Heartbeat Time: %-16s",
          DATETIME_FORMAT.format(Instant.ofEpochMilli(proxyStatus.getLastHeartbeatTime())));
      mPrintStream.printf("Proxy Version: %-32s", proxyStatus.getVersion());
      mPrintStream.printf("Proxy Revision: %-8s%n", proxyStatus.getRevision());
    }
    return 0;
  }
}
