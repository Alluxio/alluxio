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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Abstract class for handling command line inputs.
 */
@NotThreadSafe
public abstract class AbstractShell implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractShell.class);

  private Map<String, ? extends Command> mCommands;

  /**
   * Creates a new instance of {@link AbstractShell}.
   */
  public AbstractShell() {
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
    Command command = getCommands().get(cmd);

    if (command == null) { // Unknown command (we didn't find the cmd in our dict)
      System.out.println(String.format("%s is an unknown command.", cmd));
      printUsage();
      return -1;
    }

    String[] args = Arrays.copyOfRange(argv, 1, argv.length);
    CommandLine cmdline = command.parseAndValidateArgs(args);
    if (cmdline == null) {
      System.out.println("Usage: " + command.getUsage());
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

  @Override
  public void close() throws IOException {
  }

  /**
   * Load commands if not already initialized.
   *
   * @return set of commands supported in shell
   */
  protected Map<String, ? extends Command> getCommands() {
    if (mCommands == null) {
      mCommands = loadCommands();
    }
    return mCommands;
  }

  /**
   * @return name of the shell
   */
  protected abstract String getShellName();

  /**
   * @return a set of commands which can be executed under this shell
   */
  protected abstract Map<String, ? extends Command> loadCommands();

  /**
   * Prints usage for all commands.
   */
  protected void printUsage() {
    System.out.println("Usage: alluxio " + getShellName() + " [generic options]");
    SortedSet<String> sortedCmds = new TreeSet<>(getCommands().keySet());
    for (String cmd : sortedCmds) {
      System.out.format("%-60s%n", "\t [" + getCommands().get(cmd).getUsage() + "]");
    }
  }
}
