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

package alluxio.cli.fs.command;

import alluxio.annotation.PublicApi;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.jline.terminal.TerminalBuilder;

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
@PublicApi
public final class HelpCommand extends AbstractFileSystemCommand {
  private static final HelpFormatter HELP_FORMATTER = new HelpFormatter();

  /**
   * Prints the info about a command to the given print writer.
   *
   * @param command command to print info
   * @param pw where to print the info
   */
  public static void printCommandInfo(Command command, PrintWriter pw) {
    String description =
        String.format("%s: %s", command.getCommandName(), command.getDescription());
    int width = 80;
    try {
      width = TerminalBuilder.terminal().getWidth();
    } catch (Exception e) {
      // In case the terminal builder failed to decide terminal type, use default width
    }
    // Use default value if terminal width is assigned 0
    if (width == 0) {
      width = 80;
    }
    HELP_FORMATTER.printWrapped(pw, width, description);
    HELP_FORMATTER.printUsage(pw, width, command.getUsage());
    if (command.getOptions().getOptions().size() > 0) {
      HELP_FORMATTER.printOptions(pw, width, command.getOptions(), HELP_FORMATTER.getLeftPadding(),
          HELP_FORMATTER.getDescPadding());
    }
  }

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public HelpCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "help";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    SortedSet<String> sortedCmds;
    Map<String, Command> commands = FileSystemShellUtils.loadCommands(mFsContext);
    try (PrintWriter pw = new PrintWriter(System.out)) {
      if (args.length == 0) {
        // print help messages for all supported commands.
        sortedCmds = new TreeSet<>(commands.keySet());
        for (String commandName : sortedCmds) {
          Command command = commands.get(commandName);
          printCommandInfo(command, pw);
          pw.println();
        }
      } else if (commands.containsKey(args[0])) {
        Command command = commands.get(args[0]);
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
    return "help [<command>]";
  }

  @Override
  public String getDescription() {
    return "Prints help message for the given command. "
        + "If there isn't given command, prints help messages for all supported commands.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoMoreThan(this, cl, 1);
  }
}
