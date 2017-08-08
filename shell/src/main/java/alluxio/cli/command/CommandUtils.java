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

package alluxio.cli.command;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import org.reflections.Reflections;

import com.google.common.base.Throwables;

import alluxio.util.CommonUtils;

/**
 * Class for convenience methods used by {@link Command}.
 */
@ThreadSafe
public final class CommandUtils {

  private CommandUtils() {} // prevent instantiation

  /**
   * Gets all supported {@link Command} classes instances in same package and load them into a map.
   *
   * @return a mapping from command name to command instance
   */
  public static Map<String, Command> loadCommands(Class command, Class[] classArgs,
      Object[] objectArgs) {
    String pkgName = command.getPackage().getName();
    Map<String, Command> commandsMap = new HashMap<>();
    Reflections reflections = new Reflections(pkgName);
    for (Class<? extends Command> cls : reflections.getSubTypesOf(Command.class)) {
      // Only instantiate a concrete class
      if (!Modifier.isAbstract(cls.getModifiers())) {
        Command cmd;
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
