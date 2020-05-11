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

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Install a new extension.
 */
@ThreadSafe
public final class InstallCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(InstallCommand.class);

  /**
   * Constructs a new instance of {@link InstallCommand}.
   */
  public InstallCommand() {}

  @Override
  public String getCommandName() {
    return "install";
  }

  @Override
  public String getUsage() {
    return "install <URI>";
  }

  @Override
  public String getDescription() {
    return "Installs an extension into hosts configured in conf/masters and conf/workers.";
  }

  @Override
  public int run(CommandLine cl) {
    String uri = cl.getArgs()[0];
    AlluxioConfiguration conf = ServerConfiguration.global();
    String extensionsDir = conf.get(PropertyKey.EXTENSIONS_DIR);
    File dir = new File(extensionsDir);
    if (!dir.exists() && !dir.mkdirs()) {
      System.err.println("Failed to create extensions directory " + extensionsDir);
      return -1;
    }
    List<String> failedHosts = new ArrayList<>();
    for (String host : ConfigurationUtils.getServerHostnames(conf)) {
      try {
        LOG.info("Attempting to install extension on host {}", host);
        // Parent folder on target host
        String targetUriParent =
            extensionsDir.endsWith("/") ? extensionsDir : extensionsDir.concat("/");
        String rsyncCmd = String.format("rsync -e \"ssh %s\" -az %s %s:%s",
            ShellUtils.COMMON_SSH_OPTS, uri, host, targetUriParent);
        LOG.debug("Executing: {}", rsyncCmd);
        String output = ShellUtils.execCommand("bash", "-c", rsyncCmd);
        LOG.debug("Succeeded w/ output: {}", output);
      } catch (IOException e) {
        LOG.error("Error installing extension on host {}", host, e);
        failedHosts.add(host);
      }
    }
    if (failedHosts.size() != 0) {
      System.err.println("Failed to install extension on hosts:");
      for (String failedHost : failedHosts) {
        System.err.println(failedHost);
      }
      return -1;
    }
    System.out.println("Extension installed successfully.");
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
