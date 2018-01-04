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

package alluxio.cli.fsadmin;

import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemMasterClient;

import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for convenience methods used by instances of {@link ExtensionsShell}.
 */
@ThreadSafe
public final class FileSystemAdminShellUtils {

  private FileSystemAdminShellUtils() {} // prevent instantiation

  /**
   * Gets all {@link Command} instances in the same package as {@link ExtensionsShell} and loads
   * them into a map.
   *
   * @return a mapping from command name to command instance
   */
  public static Map<String, Command> loadCommands(FileSystemMasterClient masterClient) {
    return CommandUtils.loadCommands(FileSystemAdminShell.class.getPackage().getName(),
        new Class[] {FileSystemMasterClient.class}, new Object[] {masterClient});
  }
}
