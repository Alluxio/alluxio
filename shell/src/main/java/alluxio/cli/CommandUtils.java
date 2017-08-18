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

import alluxio.util.CommonUtils;

import com.google.common.base.Throwables;
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
   * Gets all subclasses of the given {@link Command} class in the same package.
   *
   * @param <T> type of command
   * @param command class to load
   * @param classArgs type of args to instantiate the class
   * @param objectArgs args to instantiate the class
   * @return a mapping from command name to command instance
   */
  public static <T extends Command> Map<String, T> loadCommands(Class<T> command, Class[] classArgs,
      Object[] objectArgs) {
    String pkgName = command.getPackage().getName();
    Map<String, T> commandsMap = new HashMap<>();
    Reflections reflections = new Reflections(pkgName);
    for (Class<? extends T> cls : reflections.getSubTypesOf(command)) {
      if (!Modifier.isAbstract(cls.getModifiers())) {
        // Only instantiate a concrete class
        T cmd;
        try {
          cmd = CommonUtils.createNewClassInstance(cls, classArgs, objectArgs);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        commandsMap.put(cmd.getCommandName(), cmd);
      }
    }
    return commandsMap;
  }
}
