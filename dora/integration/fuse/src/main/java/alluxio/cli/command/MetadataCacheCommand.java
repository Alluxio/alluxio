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

import alluxio.Constants;
import alluxio.cli.Command;
import alluxio.cli.command.metadatacache.DropAllCommand;
import alluxio.cli.command.metadatacache.DropCommand;
import alluxio.cli.command.metadatacache.SizeCommand;
import alluxio.client.file.FileSystem;
import alluxio.collections.TwoKeyConcurrentMap;
import alluxio.conf.AlluxioConfiguration;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Gets information of or makes changes to the Fuse client metadata cache.
 */
@ThreadSafe
public final class MetadataCacheCommand extends AbstractFuseShellCommand {

  private static final Map<String, TwoKeyConcurrentMap.TriFunction<FileSystem,
      AlluxioConfiguration, String, ? extends Command>> SUB_COMMANDS = new HashMap<>();

  static {
    SUB_COMMANDS.put("dropAll", DropAllCommand::new);
    SUB_COMMANDS.put("drop", DropCommand::new);
    SUB_COMMANDS.put("size", SizeCommand::new);
  }

  private final Map<String, Command> mSubCommands = new HashMap<>();

  /**
   * @param fs filesystem instance from fuse command
   * @param conf the Alluxio configuration
   */
  public MetadataCacheCommand(FileSystem fs,
      AlluxioConfiguration conf) {
    super(fs, conf, "");
    SUB_COMMANDS.forEach((name, constructor)
        -> mSubCommands.put(name, constructor.apply(fs, conf, getCommandName())));
  }

  @Override
  public Map<String, Command> getSubCommands() {
    return mSubCommands;
  }

  @Override
  public String getCommandName() {
    return "metadatacache";
  }

  @Override
  public String getUsage() {
    // Show usage: metadatacache.(drop|size|dropAll)
    StringBuilder usage = new StringBuilder("ls -l " + Constants.DEAFULT_FUSE_MOUNT
        + Constants.ALLUXIO_CLI_PATH + "." + getCommandName() + ".(");
    for (String cmd : SUB_COMMANDS.keySet()) {
      usage.append(cmd).append("|");
    }
    usage.deleteCharAt(usage.length() - 1).append(')');
    return usage.toString();
  }

  @Override
  public String getDescription() {
    return "Metadatacache command is used to drop metadata cache or get cache size.";
  }
}
