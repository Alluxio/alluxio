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

package alluxio.cli.fsadmin.command;

import alluxio.cli.Command;
import alluxio.cli.fsadmin.nodes.WorkerStatusCommand;
import alluxio.conf.AlluxioConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Command to get or administrate worker nodes status.
 */
public class NodesCommand extends AbstractFsAdminCommand {

  private static final Map<String, BiFunction<Context, AlluxioConfiguration, ? extends Command>>
      SUB_COMMANDS = new HashMap<>();

  static {
    SUB_COMMANDS.put("status", WorkerStatusCommand::new);
  }

  private Map<String, Command> mSubCommands = new HashMap<>();

  /**
   * @param context
   * @param alluxioConf
   */
  public NodesCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    SUB_COMMANDS.forEach((name, constructor) -> {
      mSubCommands.put(name, constructor.apply(context, alluxioConf));
    });
  }

  @Override
  public String getCommandName() {
    return "nodes";
  }

  @Override
  public Map<String, Command> getSubCommands() {
    return mSubCommands;
  }

  @Override
  public String getUsage() {
    StringBuilder usage = new StringBuilder(getCommandName());
    for (String cmd : SUB_COMMANDS.keySet()) {
      usage.append(" [").append(cmd).append("]");
    }
    return usage.toString();
  }

  @Override
  public String getDescription() {
    return "Provide operations for worker nodes. "
        + "See sub-commands' descriptions for more details.";
  }
}
