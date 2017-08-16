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
import alluxio.util.io.PathUtils;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Uninstall an extension.
 */
@ThreadSafe
public final class UninstallCommand extends AbstractExtensionCommand {
  private static final Logger LOG = LoggerFactory.getLogger(UninstallCommand.class);

  public UninstallCommand() {}

  @Override
  public String getCommandName() {
    return "uninstall";
  }

  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public String getUsage() {
    return "uninstall <URI>";
  }

  @Override
  public String getDescription() {
    return "Uninstalls an extension.";
  }

  @Override
  public int run(CommandLine cl) {
    try {
      String uri = cl.getArgs()[0];
      String extensionDir = Configuration.get(PropertyKey.EXTENSION_DIR);
      for (String host : ExtensionUtils.getServerHostnames()) {
        String rmCmd = String.format("ssh %s %s rm %s", ExtensionUtils.SSH_OPTS, host,
            PathUtils.concatPath(extensionDir, uri));
        LOG.info("Executing: {}", rmCmd);
        String output = ShellUtils.execCommand("bash", "-c", rmCmd);
        LOG.info("Succeeded w/ output: {}", output);
      }
    } catch (IOException e) {
      LOG.error("Error uninstalling extension.", e);
      System.out.println("Failed to uninstall extension.");
      return -1;
    }
    System.out.println("Extension uninstalled successfully.");
    return 0;
  }

  @Override
  public boolean validateArgs(String... args) {
    return args.length == 1 && args[0].endsWith(EXTENSION_JAR);
  }
}