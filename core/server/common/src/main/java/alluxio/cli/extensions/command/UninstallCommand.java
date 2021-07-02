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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ShellUtils;
import alluxio.util.io.PathUtils;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Uninstall an extension.
 */
@ThreadSafe
public final class UninstallCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(UninstallCommand.class);

  /**
   * Constructs a new instance of {@link UninstallCommand}.
   */
  public UninstallCommand() {}

  @Override
  public String getCommandName() {
    return "uninstall";
  }

  @Override
  public String getUsage() {
    return "uninstall <JAR>";
  }

  @Override
  public String getDescription() {
    return "Uninstalls an extension from hosts configured in conf/masters and conf/workers.";
  }

  @Override
  public int run(CommandLine cl) {
    String uri = cl.getArgs()[0];
    AlluxioConfiguration conf = ServerConfiguration.global();
    String extensionsDir = conf.get(PropertyKey.EXTENSIONS_DIR);
    List<String> failedHosts = new ArrayList<>();
    for (String host : ConfigurationUtils.getServerHostnames(conf)) {
      try {
        LOG.info("Attempting to uninstall extension on host {}", host);
        String rmCmd = String.format("ssh %s %s rm %s", ShellUtils.COMMON_SSH_OPTS, host,
            PathUtils.concatPath(extensionsDir, uri));
        LOG.debug("Executing: {}", rmCmd);
        String output = ShellUtils.execCommand("bash", "-c", rmCmd);
        LOG.debug("Succeeded w/ output: {}", output);
      } catch (IOException e) {
        LOG.error("Error uninstalling extension on host {}.", host, e);
        failedHosts.add(host);
      }
    }
    if (failedHosts.size() != 0) {
      System.err.println("Failed to uninstall extension on hosts:");
      for (String failedHost : failedHosts) {
        System.err.println(failedHost);
      }
      return -1;
    }
    System.out.println("Extension uninstalled successfully.");
    return 0;
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    String[] args = cl.getArgs();
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
    if (args[0] == null) {
      throw new InvalidArgumentException(
          ExceptionMessage.INVALID_ARGS_NULL.getMessage(getCommandName()));
    }
    if (!args[0].endsWith(Constants.EXTENSION_JAR)) {
      throw new InvalidArgumentException(
          ExceptionMessage.INVALID_EXTENSION_NOT_JAR.getMessage(args[0]));
    }
  }
}
