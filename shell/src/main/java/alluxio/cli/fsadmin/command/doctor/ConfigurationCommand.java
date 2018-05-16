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

package alluxio.cli.fsadmin.command.doctor;

import alluxio.client.MetaMasterClient;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Prints runtime configuration information.
 */
public class ConfigurationCommand {
  private MetaMasterClient mMetaMasterClient;
  private PrintStream mPrintStream;

  /**
   * Creates a new instance of {@link ConfigurationCommand}.
   *
   * @param metaMasterClient client to get server-side configuration errors/warnings
   * @param printStream stream to print configuration errors/warnings to
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

    return 0;
  }
}
