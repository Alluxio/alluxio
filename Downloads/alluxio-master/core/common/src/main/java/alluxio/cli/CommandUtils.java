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

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for convenience methods used by instances of {@link Command}.
 */
@ThreadSafe
public final class CommandUtils {

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
}
