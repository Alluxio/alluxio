/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.shell.command.ShellCommand;
import alluxio.util.CommonUtils;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class for handling command line inputs.
 */
@NotThreadSafe
public class AlluxioShell implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final Map<String, String[]> CMD_ALIAS = ImmutableMap.<String, String[]>builder()
      .put("chgrpr", new String[] {"chgrp", "-R"})
      .put("chmodr", new String[] {"chmod", "-R"})
      .put("chownr", new String[] {"chown", "-R"})
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
    AlluxioShell shell = new AlluxioShell(new Configuration());
    int ret;
    try {
      ret = shell.run(argv);
    } finally {
      shell.close();
    }
    System.exit(ret);
  }

  private final Map<String, ShellCommand> mCommands = Maps.newHashMap();
  private final Configuration mConfiguration;
  private final FileSystem mFileSystem;

  /**
   * @param configuration the configuration for Alluxio
   */
  public AlluxioShell(Configuration configuration) {
    mConfiguration = configuration;
    mFileSystem = FileSystem.Factory.get();
    loadCommands();
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * Uses reflection to get all the {@link ShellCommand} classes and store them in a map.
   */
  private void loadCommands() {
    String pkgName = ShellCommand.class.getPackage().getName();
    Reflections reflections = new Reflections(pkgName);
    for (Class<? extends ShellCommand> cls : reflections.getSubTypesOf(ShellCommand.class)) {
      // Only instantiate a concrete class
      if (!Modifier.isAbstract(cls.getModifiers())) {
        ShellCommand cmd;
        try {
          cmd = CommonUtils.createNewClassInstance(cls,
              new Class[] { Configuration.class, FileSystem.class },
              new Object[] {mConfiguration, mFileSystem });
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        mCommands.put(cmd.getCommandName(), cmd);
      }
    }
  }

  /**
   * Gets the replacement command for alias.
   *
   * @param command name
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
   * Method which prints the method to use all the commands.
   */
  private void printUsage() {
    System.out.println("Usage: java AlluxioShell");
    SortedSet<String> sortedCmds = new TreeSet<String>(mCommands.keySet());
    for (String cmd : sortedCmds) {
      System.out.format("%-60s%-95s%n", "       [" + mCommands.get(cmd).getUsage() + "]   ",
          mCommands.get(cmd).getDescription());
    }
  }

  /**
   * Method which determines how to handle the user's request, will display usage help to the user
   * if command format is incorrect.
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
      command.run(cmdline);
      return 0;
    } catch (IOException e) {
      System.out.println(e.getMessage());
      LOG.error("Error running " + StringUtils.join(argv, " "), e);
      return -1;
    }
  }
}
