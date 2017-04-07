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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.shell.AlluxioShellUtils;
import alluxio.shell.command.ShellCommand;
import alluxio.util.ConfigurationUtils;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class for handling command line inputs.
 */
@NotThreadSafe
public final class AlluxioShell implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioShell.class);

  private static final Map<String, String[]> CMD_ALIAS = ImmutableMap.<String, String[]>builder()
      .put("lsr", new String[] {"ls", "-R"})
      .put("rmr", new String[] {"rm", "-R"})
      .build();

  /**
   * Main method, starts a new AlluxioShell.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @throws IOException if closing the shell fails
   */
  public static void main(String[] argv) throws IOException {
    int ret;

    if (!ConfigurationUtils.masterHostConfigured()) {
      System.out.println(String.format(
          "Cannot run alluxio shell; master hostname is not "
              + "configured. Please modify %s to either set %s or configure zookeeper with "
              + "%s=true and %s=[comma-separated zookeeper master addresses]",
          Configuration.SITE_PROPERTIES, PropertyKey.MASTER_HOSTNAME.toString(),
          PropertyKey.ZOOKEEPER_ENABLED.toString(), PropertyKey.ZOOKEEPER_ADDRESS.toString()));
      System.exit(1);
    }

    try (AlluxioShell shell = new AlluxioShell()) {
      ret = shell.run(argv);
    }
    System.exit(ret);
  }

  private final Map<String, ShellCommand> mCommands = new HashMap<>();
  private final FileSystem mFileSystem;

  /**
   * Creates a new instance of {@link AlluxioShell}.
   */
  public AlluxioShell() {
    mFileSystem = FileSystem.Factory.get();
    AlluxioShellUtils.loadCommands(mFileSystem, mCommands);
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * Gets the replacement command for alias.
   *
   * @param cmd the name of the command
   * @return replacement command if cmd is an alias
   */
  private String[] getReplacementCmd(String cmd) {
    if (CMD_ALIAS.containsKey(cmd)) {
      return CMD_ALIAS.get(cmd);
    } else {
      return null;
    }
  }

  /**
   * Prints usage for all shell commands.
   */
  private void printUsage() {
    System.out.println("Usage: java AlluxioShell");
    SortedSet<String> sortedCmds = new TreeSet<>(mCommands.keySet());
    for (String cmd : sortedCmds) {
      System.out.format("%-60s%-95s%n", "       [" + mCommands.get(cmd).getUsage() + "]   ",
          mCommands.get(cmd).getDescription());
    }
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
    ShellCommand command = mCommands.get(cmd);

    if (command == null) { // Unknown command (we didn't find the cmd in our dict)
      String[] replacementCmd = getReplacementCmd(cmd);
      if (replacementCmd == null) {
        System.out.println(cmd + " is an unknown command.\n");
        printUsage();
        return -1;
      }
      // Handle command alias, and print out WARNING message for deprecated cmd.
      String deprecatedMsg = "WARNING: " + cmd + " is deprecated. Please use "
                             + StringUtils.join(replacementCmd, " ") + " instead.";
      System.out.println(deprecatedMsg);
      LOG.warn(deprecatedMsg);

      String[] replacementArgv = (String[]) ArrayUtils.addAll(replacementCmd,
          ArrayUtils.subarray(argv, 1, argv.length));
      return run(replacementArgv);
    }

    String[] args = Arrays.copyOfRange(argv, 1, argv.length);
    CommandLine cmdline = command.parseAndValidateArgs(args);
    if (cmdline == null) {
      printUsage();
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
}
