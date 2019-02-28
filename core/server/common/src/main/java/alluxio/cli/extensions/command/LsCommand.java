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

import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.ExtensionUtils;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Lists all installed extensions.
 */
@ThreadSafe
public final class LsCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(LsCommand.class);

  /**
   * Constructs a new instance of {@link LsCommand}.
   */
  public LsCommand() {
  }

  @Override
  public String getCommandName() {
    return "ls";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }

  @Override
  public String getUsage() {
    return "ls";
  }

  @Override
  public String getDescription() {
    return "Lists JAR names for all installed extensions.";
  }

  @Override
  public int run(CommandLine cl) {
    for (File extension : ExtensionUtils
        .listExtensions(ServerConfiguration.get(PropertyKey.EXTENSIONS_DIR))) {
      System.out.println(extension.getName());
    }
    return 0;
  }
}
