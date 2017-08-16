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

package alluxio.extension.command;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.extension.ExtensionUtils;
import alluxio.util.ShellUtils;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Install a new extension.
 */
@ThreadSafe
public final class InstallCommand extends AbstractExtensionCommand {
  private static final Logger LOG = LoggerFactory.getLogger(InstallCommand.class);

  /**
   * Construct a new instance of {@link InstallCommand}.
   */
  public InstallCommand() {}

  @Override
  public String getCommandName() {
    return "install";
  }

  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public String getUsage() {
    return "install <URI>";
  }

  @Override
  public String getDescription() {
    return "Installs an extension.";
  }

  @Override
  public int run(CommandLine cl) {
    try {
      String uri = cl.getArgs()[0];
      String extensionDir = Configuration.get(PropertyKey.EXTENSION_DIR);
      for (String host : ExtensionUtils.getServerHostnames()) {
        String rsyncCmd = String.format("rsync -e \"ssh %s\" -az %s %s:%s", ExtensionUtils.SSH_OPTS,
            uri, host, extensionDir);
        LOG.info("Executing: {}", rsyncCmd);
        String output = ShellUtils.execCommand("bash", "-c", rsyncCmd);
        LOG.info("Succeeded w/ output: {}", output);
      }
    } catch (IOException e) {
      LOG.error("Error installing extension.", e);
      System.out.println("Failed to install extension.");
      return -1;
    }
    System.out.println("Extension installed successfully.");
    return 0;
  }

  @Override
  public boolean validateArgs(String... args) {
    return args.length == 1 && args[0].endsWith(EXTENSION_JAR);
  }
}
