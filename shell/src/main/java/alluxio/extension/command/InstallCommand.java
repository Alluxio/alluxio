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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Prints the file's contents to the console.
 */
@ThreadSafe
public final class InstallCommand extends AbstractExtensionCommand {
  private static final Logger LOG = LoggerFactory.getLogger(InstallCommand.class);

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
    return "install <mvn coordinate | URI>";
  }

  @Override
  public String getDescription() {
    return "Installs an Alluxio extension.";
  }

  @Override
  public int run(CommandLine cl) {
    try {
      String arg = cl.getArgs()[0];
      String extensionDir = Configuration.get(PropertyKey.EXTENSION_DIR);
      System.out.println("Copying extension from " + arg + " into " + extensionDir);
      FileUtils.copyFileToDirectory(new File(arg), new File(extensionDir));
    } catch (IOException e) {
      LOG.error("Error installing extension.", e);
      return -1;
    }
    System.out.println("Extension installed successfully.");
    return 0;
  }

  @Override
  public boolean validateArgs(String... args) {
    return args.length >= 1;
  }
}
