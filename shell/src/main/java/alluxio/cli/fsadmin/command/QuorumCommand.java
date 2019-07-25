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
import alluxio.cli.fsadmin.quorum.QuorumInfoCommand;
import alluxio.cli.fsadmin.quorum.QuorumRemoveCommand;
import alluxio.conf.AlluxioConfiguration;

import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Command for seeing/managing quorum state of embedded journal.
 */
public class QuorumCommand extends AbstractFsAdminCommand {

  private static final Map<String, BiFunction<Context, AlluxioConfiguration, ? extends Command>>
      SUB_COMMANDS = new HashMap<>();

  static {
    SUB_COMMANDS.put("info", QuorumInfoCommand::new);
    SUB_COMMANDS.put("remove", QuorumRemoveCommand::new);
  }

  private Map<String, Command> mSubCommands = new HashMap<>();

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public QuorumCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    SUB_COMMANDS.forEach((name, constructor) -> {
      mSubCommands.put(name, constructor.apply(context, alluxioConf));
    });
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Manage embedded journal quorum configuration. "
        + "See sub-commands' descriptions for more details.";
  }

  @Override
  public boolean hasSubCommand() {
    return true;
  }

  @Override
  public Map<String, Command> getSubCommands() {
    return mSubCommands;
  }

  @Override
  public String getCommandName() {
    return "quorum";
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
    return description();
  }
}
