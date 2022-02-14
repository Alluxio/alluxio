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

package alluxio.cli.command.metric;

import alluxio.Constants;
import alluxio.cli.Command;
import alluxio.cli.command.AbstractFuseShellCommand;
import alluxio.cli.command.metric.totalcalls.TotalCountCommand;
import alluxio.client.file.FileSystem;
import alluxio.collections.TwoKeyConcurrentMap;
import alluxio.conf.AlluxioConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * The total calls metric subcommand.
 */
public final class TotalCallsCommand extends AbstractFuseShellCommand {
  private static final Map<String, TwoKeyConcurrentMap.TriFunction<FileSystem,
      AlluxioConfiguration, String, ? extends Command>> SUB_COMMANDS = new HashMap<>();

  static {
    SUB_COMMANDS.put("count", TotalCountCommand::new);
  }

  private final Map<String, Command> mSubCommands = new HashMap<>();

  /**
   * @param fs filesystem instance from fuse command
   * @param conf configuration instance from fuse command
   * @param commandName the parent command name
   */
  public TotalCallsCommand(FileSystem fs, AlluxioConfiguration conf, String commandName) {
    super(fs, conf, commandName);
    SUB_COMMANDS.forEach((name, constructor) -> {
      mSubCommands.put(name, constructor.apply(fs, conf, getCommandName()));
    });
  }

  @Override
  public Map<String, Command> getSubCommands() {
    return mSubCommands;
  }

  /**
   * Get command name.
   * @return the command name
   */
  public String getCommandName() {
    return "totalCalls";
  }

  @Override
  public String getUsage() {
    StringBuilder usage = new StringBuilder("ls -l " + Constants.DEAFULT_FUSE_MOUNT
        + Constants.ALLUXIO_CLI_PATH + ".fusemetric." + getCommandName() + ".(");
    for (String cmd : getSubCommands().keySet()) {
      usage.append(cmd).append("|");
    }
    usage.deleteCharAt(usage.length() - 1).append(')');
    return usage.toString();
  }

  @Override
  public String getDescription() {
    return "Get total calls request metric.";
  }
}
