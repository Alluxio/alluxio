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

package alluxio.cli.extensions.command;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.cli.AbstractCommand;
import alluxio.cli.extensions.ExtensionsShellUtils;
import alluxio.util.ShellUtils;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Install a new extension.
 */
@ThreadSafe
public final class LsCommand extends AbstractCommand {
  private static final Logger LOG = LoggerFactory.getLogger(LsCommand.class);

  /**
   * Constructs a new instance of {@link LsCommand}.
   */
  public LsCommand() {}

  @Override
  public String getCommandName() {
    return "ls";
  }

  protected int getNumOfArgs() {
    return 0;
  }

  @Override
  public String getUsage() {
    return "ls";
  }

  @Override
  public String getDescription() {
    return "Lists all installed extensions.";
  }

  @Override
  public int run(CommandLine cl) {
    try {
      String extensionDir = Configuration.get(PropertyKey.EXTENSION_DIR);
      List<String> masters = ExtensionsShellUtils.getMasterHostnames();
      if (masters == null) {
        System.out.println("Unable to find a master in conf/masters");
        return -1;
      }
      String host = masters.get(0);
      String lsCmd =
          String.format("ssh %s %s ls %s", ShellUtils.COMMON_SSH_OPTS, host, extensionDir);
      LOG.info("Executing: {}", lsCmd);
      String output = ShellUtils.execCommand("bash", "-c", lsCmd);
      System.out.println("| Extension URI |");
      System.out.print(output);
    } catch (IOException e) {
      LOG.error("Error installing extension.", e);
      System.out.println("Failed to install extension.");
      return -1;
    }
    return 0;
  }

  @Override
  public boolean validateArgs(String... args) {
    return args.length == 0;
  }
}
