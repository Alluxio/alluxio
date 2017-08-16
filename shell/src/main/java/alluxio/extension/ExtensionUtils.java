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

package alluxio.extension;

import alluxio.cli.ExtensionShell;
import alluxio.cli.CommandUtils;
import alluxio.cli.validation.Utils;
import alluxio.extension.command.ExtensionCommand;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for convenience methods used by instances of {@link ExtensionShell}.
 */
@ThreadSafe
public final class ExtensionUtils {

  private ExtensionUtils() {} // prevent instantiation

  private static final String MASTERS = "masters";
  private static final String WORKERS = "workers"; 
  
  /**
   * Gets all supported {@link ExtensionCommand} classes instances and load them into a map.
   *
   * @return a mapping from command name to command instance
   */
  public static Map<String, ExtensionCommand> loadCommands() {
    return CommandUtils.loadCommands(ExtensionCommand.class, null, null);
  }

  public static List<String> getMasterHostnames() {
    return Utils.readNodeList(MASTERS);
  }
  
  public static Set<String> getServerHostnames() {
    List<String> masters = getMasterHostnames();
    List<String> workers = getWorkerHostnames();

    Set<String> hostnames = new HashSet<>(masters.size() + workers.size());
    hostnames.addAll(masters);
    hostnames.addAll(workers);
    return hostnames;
  }
  
  public static List<String> getWorkerHostnames() {
    return Utils.readNodeList(WORKERS);
  }
}