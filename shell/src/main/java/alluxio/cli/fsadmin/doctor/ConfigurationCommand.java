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

package alluxio.cli.fsadmin.doctor;

import alluxio.client.meta.MetaMasterClient;
import alluxio.grpc.ConfigStatus;
import alluxio.grpc.Scope;
import alluxio.wire.ConfigCheckReport;
import alluxio.wire.InconsistentProperty;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Prints server-side configuration errors and warnings.
 */
public class ConfigurationCommand {
  private final MetaMasterClient mMetaMasterClient;
  private final PrintStream mPrintStream;

  /**
   * Creates a new instance of {@link ConfigurationCommand}.
   *
   * @param metaMasterClient client to get server-side configuration report information
   * @param printStream stream to print configuration errors/warnings to
   */
  public ConfigurationCommand(MetaMasterClient metaMasterClient, PrintStream printStream) {
    mMetaMasterClient = metaMasterClient;
    mPrintStream = printStream;
  }

  /**
   * Runs doctor configuration command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    ConfigCheckReport report = mMetaMasterClient.getConfigReport();
    ConfigStatus configStatus = report.getConfigStatus();
    if (configStatus == ConfigStatus.PASSED) {
      // No errors or warnings to show
      mPrintStream.println("No server-side configuration errors or warnings.");
      return 0;
    }

    Map<Scope, List<InconsistentProperty>> errors = report.getConfigErrors();
    if (errors.size() != 0) {
      mPrintStream.println("Server-side configuration errors "
          + "(those properties are required to be identical): ");
      printInconsistentProperties(errors);
    }

    Map<Scope, List<InconsistentProperty>> warnings = report.getConfigWarns();
    if (warnings.size() != 0) {
      mPrintStream.println("\nServer-side configuration warnings "
          + "(those properties are recommended to be identical): ");
      printInconsistentProperties(warnings);
    }
    return 0;
  }

  /**
   * Prints the inconsistent properties in server-side configuration.
   *
   * @param inconsistentProperties the inconsistent properties to print
   */
  private void printInconsistentProperties(
      Map<Scope, List<InconsistentProperty>> inconsistentProperties) {
    for (List<InconsistentProperty> list : inconsistentProperties.values()) {
      for (InconsistentProperty prop : list) {
        mPrintStream.println("key: " + prop.getName());
        for (Map.Entry<Optional<String>, List<String>> entry : prop.getValues().entrySet()) {
          mPrintStream.println("    value: " + String.format("%s (%s)",
              entry.getKey().orElse("no value set"),
              String.join(", ", entry.getValue())));
        }
      }
    }
  }
}
