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

import alluxio.client.MetaMasterClient;
import alluxio.wire.ConfigProperty;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Prints server-side configuration check report.
 */
public class ConfigurationCommand {
  private static final String CONFIG_INFO_FORMAT = "%s = %s (source = %s)%n";
  private MetaMasterClient mMetaMasterClient;
  private PrintStream mPrintStream;

  /**
   * Creates a new instance of {@link ConfigurationCommand}.
   *
   * @param metaMasterClient client to get list of config info from
   * @param printStream stream to print runtime configuration information to
   */
  public ConfigurationCommand(MetaMasterClient metaMasterClient, PrintStream printStream) {
    mMetaMasterClient = metaMasterClient;
    mPrintStream = printStream;
  }

  /**
   * Runs report configuration command.
   */
  public void run() throws IOException {
    List<ConfigProperty> configList = mMetaMasterClient.getConfiguration();
    Collections.sort(configList, Comparator.comparing(ConfigProperty::getSource));

    mPrintStream.print("Alluxio configuration information: \n");

    for (ConfigProperty info : configList) {
      mPrintStream.print(String.format(CONFIG_INFO_FORMAT,
          info.getName(), info.getValue(), info.getSource()));
    }
  }
}
