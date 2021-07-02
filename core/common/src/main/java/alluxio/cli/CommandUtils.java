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

import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.CommonUtils;

import org.apache.commons.cli.CommandLine;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for convenience methods used by instances of {@link Command}.
 */
@ThreadSafe
public final class CommandUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CommandUtils.class);

  private CommandUtils() {} // prevent instantiation

  /**
   * Get instances of all subclasses of {@link Command} in a sub-package called "command" the given
   * package.
   *
   * @param pkgName package prefix to look in
   * @param classArgs type of args to instantiate the class
   * @param objectArgs args to instantiate the class
   * @return a mapping from command name to command instance
   */
  public static Map<String, Command> loadCommands(String pkgName, Class[] classArgs,
      Object[] objectArgs) {
    Map<String, Command> commandsMap = new HashMap<>();
    Reflections reflections = new Reflections(Command.class.getPackage().getName());
    for (Class<? extends Command> cls : reflections.getSubTypesOf(Command.class)) {
      // Add commands from <pkgName>.command.*
      if (cls.getPackage().getName().equals(pkgName + ".command")
          && !Modifier.isAbstract(cls.getModifiers())) {
        // Only instantiate a concrete class
        Command cmd = CommonUtils.createNewClassInstance(cls, classArgs, objectArgs);
        commandsMap.put(cmd.getCommandName(), cmd);
      }
    }
    return commandsMap;
  }

  /**
   * Checks the number of non-option arguments equals n for command.
   *
   * @param cmd command instance
   * @param cl parsed commandline arguments
   * @param n an integer
   * @throws InvalidArgumentException if the number does not equal n
   */
  public static void checkNumOfArgsEquals(Command cmd, CommandLine cl, int n) throws
      InvalidArgumentException {
    if (cl.getArgs().length != n) {
      throw new InvalidArgumentException(ExceptionMessage.INVALID_ARGS_NUM
          .getMessage(cmd.getCommandName(), n, cl.getArgs().length));
    }
  }

  /**
   * Checks the number of non-option arguments is no less than n for command.
   *
   * @param cmd command instance
   * @param cl parsed commandline arguments
   * @param n an integer
   * @throws InvalidArgumentException if the number is smaller than n
   */
  public static void checkNumOfArgsNoLessThan(Command cmd, CommandLine cl, int n) throws
      InvalidArgumentException {
    if (cl.getArgs().length < n) {
      throw new InvalidArgumentException(ExceptionMessage.INVALID_ARGS_NUM_INSUFFICIENT
          .getMessage(cmd.getCommandName(), n, cl.getArgs().length));
    }
  }

  /**
   * Checks the number of non-option arguments is no more than n for command.
   *
   * @param cmd command instance
   * @param cl parsed commandline arguments
   * @param n an integer
   * @throws InvalidArgumentException if the number is greater than n
   */
  public static void checkNumOfArgsNoMoreThan(Command cmd, CommandLine cl, int n) throws
      InvalidArgumentException {
    if (cl.getArgs().length > n) {
      throw new InvalidArgumentException(ExceptionMessage.INVALID_ARGS_NUM_TOO_MANY
          .getMessage(cmd.getCommandName(), n, cl.getArgs().length));
    }
  }

  /**
   * Reads a list of nodes from given file name ignoring comments and empty lines.
   * Can be used to read conf/workers or conf/masters.
   * @param confDir directory that holds the configuration
   * @param fileName name of a file that contains the list of the nodes
   * @return list of the node names, null when file fails to read
   */
  @Nullable
  public static Set<String> readNodeList(String confDir, String fileName) {
    List<String> lines;
    String path = Paths.get(confDir, fileName).normalize().toString();
    try {
      lines = Files.readAllLines(Paths.get(confDir, fileName), StandardCharsets.UTF_8);
    } catch (IOException e) {
      System.err.format("Failed to read file %s/%s. Ignored.%n", confDir, fileName);
      return new HashSet<>();
    }

    Set<String> nodes = new HashSet<>();
    for (String line : lines) {
      String node = line.trim();
      if (node.startsWith("#") || node.length() == 0) {
        continue;
      }
      if (!nodes.add(node)) {
        System.out.format("Duplicate node hostname %s found in %s%n", node, path);
      }
    }

    return nodes;
  }
}
