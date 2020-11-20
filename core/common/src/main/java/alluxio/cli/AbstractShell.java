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

package alluxio.cli;

import alluxio.conf.InstancedConfiguration;
import alluxio.exception.status.InvalidArgumentException;

import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Abstract class for handling command line inputs.
 */
@NotThreadSafe
public abstract class AbstractShell implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractShell.class);

  private Map<String, String[]> mCommandAlias;
  private Set<String> mUnstableAlias;
  private Map<String, Command> mCommands;
  protected InstancedConfiguration mConfiguration;
  protected Closer mCloser;

  /**
   * Creates a new instance of {@link AbstractShell}.
   *
   * @param commandAlias replacements for commands
   * @param unstableAlias set of unstable aliases which may be removed in the future
   * @param conf Alluxio configuration
   */
  public AbstractShell(Map<String, String[]> commandAlias,
      Set<String> unstableAlias, InstancedConfiguration conf) {
    mCloser = Closer.create();
    mConfiguration = conf; // This needs to go first in case loadCommands() uses the reference to
    // the configuration
    mUnstableAlias = unstableAlias;
    mCommandAlias = commandAlias;
    mCommands = loadCommands();
    // Register all loaded commands under closer.
    mCommands.values().stream().forEach((cmd) -> mCloser.register(cmd));
  }

  /**
   * Handles the specified shell command request, displaying usage if the command format is invalid.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred
   */
  public int run(String... argv) {
    if (argv.length == 0) {
      printUsage();
      return -1;
    }

    // Sanity check on the number of arguments
    String cmd = argv[0];
    Command command = mCommands.get(cmd);

    if (command == null) {
      String[] replacementCmd = getReplacementCmd(cmd);
      if (replacementCmd == null) {
        // Unknown command (we didn't find the cmd in our dict)
        System.err.println(String.format("%s is an unknown command.", cmd));
        printUsage();
        return -1;
      } else {
        // Handle command alias
        if (mUnstableAlias != null && mUnstableAlias.contains(cmd)) {
          String deprecatedMsg =
              String.format("WARNING: %s is not a stable CLI command. It may be removed in the "
                      + "future. Use with caution in scripts. You may use '%s' instead.",
                  cmd, StringUtils.join(replacementCmd, " "));
          System.out.println(deprecatedMsg);
        }

        String[] replacementArgv =
            (String[]) ArrayUtils.addAll(replacementCmd, ArrayUtils.subarray(argv, 1, argv.length));
        return run(replacementArgv);
      }
    }

    // Find the inner-most command and its argument line.
    CommandLine cmdline;
    try {
      String[] currArgs = Arrays.copyOf(argv, argv.length);
      while (command.hasSubCommand()) {
        if (currArgs.length < 2) {
          throw new InvalidArgumentException("No sub-command is specified");
        }
        if (!command.getSubCommands().containsKey(currArgs[1])) {
          throw new InvalidArgumentException("Unknown sub-command: " + currArgs[1]);
        }
        command = command.getSubCommands().get(currArgs[1]);
        if (currArgs.length >= 2) {
          currArgs = Arrays.copyOfRange(currArgs, 1, currArgs.length);
        }
      }
      currArgs = Arrays.copyOfRange(currArgs, 1, currArgs.length);

      cmdline = command.parseAndValidateArgs(currArgs);
    } catch (InvalidArgumentException e) {
      // It outputs a prompt message when passing wrong args to CLI
      System.out.println(e.getMessage());
      System.out.println("Usage: " + command.getUsage());
      System.out.println(command.getDescription());
      LOG.error("Invalid arguments for command {}:", command.getCommandName(), e);
      return -1;
    }

    // Handle the command
    try {
      return command.run(cmdline);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      LOG.error("Error running " + StringUtils.join(argv, " "), e);
      return -1;
    }
  }

  /**
   * @return all commands provided by this shell
   */
  public Collection<Command> getCommands() {
    return mCommands.values();
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  /**
   * Gets the replacement command for alias.
   *
   * @param cmd the name of the command
   * @return replacement command if cmd is an alias
   */
  @Nullable
  private String[] getReplacementCmd(String cmd) {
    if (mCommandAlias == null || !mCommandAlias.containsKey(cmd)) {
      return null;
    }
    return mCommandAlias.get(cmd);
  }

  /**
   * @return name of the shell
   */
  protected abstract String getShellName();

  /**
   * Map structure: Command name => {@link Command} instance.
   *
   * @return a set of commands which can be executed under this shell
   */
  protected abstract Map<String, Command> loadCommands();

  /**
   * Prints usage for all commands.
   */
  protected void printUsage() {
    System.out.println("Usage: alluxio " + getShellName() + " [generic options]");
    SortedSet<String> sortedCmds = new TreeSet<>(mCommands.keySet());
    for (String cmd : sortedCmds) {
      System.out.format("%-60s%n", "\t [" + mCommands.get(cmd).getUsage() + "]");
    }
  }
}
