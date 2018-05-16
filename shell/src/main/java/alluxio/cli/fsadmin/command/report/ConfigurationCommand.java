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

package alluxio.cli.fsadmin.command.report;

import alluxio.client.MetaMasterClient;
import alluxio.wire.ConfigProperty;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Prints runtime configuration information.
 */
public class ConfigurationCommand {
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
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    List<ConfigProperty> configList = mMetaMasterClient.getConfiguration();
    Collections.sort(configList, Comparator.comparing(ConfigProperty::getSource));
    String configInfoFormat = getConfigInfoFormat(configList);

    mPrintStream.print("Alluxio configuration information: \n");
    mPrintStream.print(String.format(configInfoFormat,
        "Property", "Value", "Source"));

    for (ConfigProperty info : configList) {
      mPrintStream.print(String.format(configInfoFormat,
          info.getName(), info.getValue(), info.getSource()));
    }
    return 0;
  }

  /**
   * Gets config info format based on the max length of each column.
   *
   * @param configList the config property list to find the max length of each column
   * @return a config info format string
   */
  private String getConfigInfoFormat(List<ConfigProperty> configList) {
    int nameSize = 15; // Default value is 15 to maintain basic column width
    int valueSize = 15;
    int sourceSize = 15;
    for (ConfigProperty property : configList) {
      nameSize = Math.max(nameSize, property.getName().length());
      valueSize = Math.max(valueSize, property.getValue().length());
      sourceSize = Math.max(sourceSize, property.getSource().length());
    }
    return new StringBuilder("%-").append(nameSize).append("s  %-")
        .append(valueSize).append("s  %-").append(sourceSize).append("s%n").toString();
  }
}
