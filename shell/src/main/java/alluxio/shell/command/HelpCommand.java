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

import jline.TerminalFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;

import java.io.IOException;
import java.io.PrintWriter;
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
  private static final HelpFormatter HELP_FORMATTER = new HelpFormatter();

  /**
   * Prints the info about a command to the given print writer.
   *
   * @param command command to print info
   * @param pw where to print the info
   */
  public static void printCommandInfo(ShellCommand command, PrintWriter pw) {
    String description =
        String.format("%s: %s", command.getCommandName(), command.getDescription());
    int width = TerminalFactory.get().getWidth();

    HELP_FORMATTER.printWrapped(pw, width, description);
    HELP_FORMATTER.printUsage(pw, width, command.getUsage());
    if (command.getOptions().getOptions().size() > 0) {
      HELP_FORMATTER.printOptions(pw, width, command.getOptions(), HELP_FORMATTER.getLeftPadding(),
          HELP_FORMATTER.getDescPadding());
    }
  }

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

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    SortedSet<String> sortedCmds;
    Map<String, ShellCommand> commands = AlluxioShellUtils.loadCommands(mFileSystem);
    try (PrintWriter pw = new PrintWriter(System.out)) {
      if (args.length == 0) {
        // print help messages for all supported commands.
        sortedCmds = new TreeSet<>(commands.keySet());
        for (String commandName : sortedCmds) {
          ShellCommand command = commands.get(commandName);
          printCommandInfo(command, pw);
          pw.println();
        }
      } else if (commands.containsKey(args[0])) {
        ShellCommand command = commands.get(args[0]);
        printCommandInfo(command, pw);
      } else {
        pw.println(args[0] + " is an unknown command.");
        return -1;
      }
      return 0;
    }
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
      System.out.println(
          getCommandName() + " takes at most " + getNumOfArgs() + " arguments, " + " not "
              + args.length + "\n");
    }
    return valid;
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }
}
