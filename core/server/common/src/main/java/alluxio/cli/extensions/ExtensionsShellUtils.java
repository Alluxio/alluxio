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

package alluxio.cli.extensions;

import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.cli.validation.Utils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for convenience methods used by instances of {@link ExtensionsShell}.
 */
@ThreadSafe
public final class ExtensionsShellUtils {

  private ExtensionsShellUtils() {} // prevent instantiation

  private static final String MASTERS = "masters";
  private static final String WORKERS = "workers";

  /**
   * Gets all {@link Command} instances in the same package as {@link ExtensionsShell} and loads
   * them into a map.
   *
   * @return a mapping from command name to command instance
   */
  public static Map<String, Command> loadCommands() {
    return CommandUtils.loadCommands(ExtensionsShell.class.getPackage().getName(), null, null);
  }

  /**
   * Gets list of masters in conf directory.
   *
   * @return master hostnames
   */
  public static List<String> getMasterHostnames() {
    return Utils.readNodeList(MASTERS);
  }

  /**
   * Gets list of masters/workers in conf directory.
   *
   * @return server hostnames
   */
  public static Set<String> getServerHostnames() {
    List<String> servers = getMasterHostnames();
    servers.addAll(getWorkerHostnames());
    return new HashSet<>(servers);
  }

  /**
   * Gets list of workers in conf directory.
   *
   * @return workers hostnames
   */
  public static List<String> getWorkerHostnames() {
    return Utils.readNodeList(WORKERS);
  }
}
