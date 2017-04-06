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

package alluxio.shell.command;

import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.shell.AlluxioShellUtils;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Command for print help message for the given command. If there isn't given command, print help
 * messages for all supported commands.
 */
@ThreadSafe
public final class HelpCommand extends AbstractShellCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public HelpCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "help";
  }

  private final Map<String, ShellCommand> mCommands = new HashMap<>();

  @Override
  public void run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    SortedSet<String> sortedCmds = null;
    AlluxioShellUtils.loadCommands(mFileSystem, mCommands);
    if (args.length == 0) {
      // print help messages for all supported commands.
      sortedCmds = new TreeSet<>(mCommands.keySet());
      for (String cmd : sortedCmds) {
        printCommandInfo(cmd);
      }
    } else if (mCommands.containsKey(args[0])) {
      printCommandInfo(args[0]);
    } else {
      System.out.println(args[0] + " is an unknown command.");
    }
  }

  private void printCommandInfo(String commandName) {
    System.out.format("%-60s%s%n", "       [" + mCommands.get(commandName).getUsage() + "]   ",
        mCommands.get(commandName).getDescription());
  }

  @Override
  public String getUsage() {
    return "help <command>";
  }

  @Override
  public String getDescription() {
    return "Prints help message for the given command. "
        + "If there isn't given command, prints help messages for all supported commands.";
  }

  @Override
  public boolean validateArgs(String... args) {
    boolean valid = args.length <= getNumOfArgs();
    if (!valid) {
      System.out.println(getCommandName() + " takes at most " + getNumOfArgs() + " arguments, "
                           + " not " + args.length + "\n");
    }
    return valid;
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

}
