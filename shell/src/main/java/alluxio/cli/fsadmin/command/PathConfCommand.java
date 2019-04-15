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
import alluxio.cli.CommandUtils;
import alluxio.cli.fsadmin.pathconf.AddCommand;
import alluxio.cli.fsadmin.pathconf.ListCommand;
import alluxio.cli.fsadmin.pathconf.RemoveCommand;
import alluxio.cli.fsadmin.pathconf.ShowCommand;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Manages path level configuration.
 */
public final class PathConfCommand extends AbstractFsAdminCommand {
  private static final Map<String, BiFunction<Context, AlluxioConfiguration, ? extends Command>>
      SUB_COMMANDS = new HashMap<>();

  static {
    SUB_COMMANDS.put("list", ListCommand::new);
    SUB_COMMANDS.put("show", ShowCommand::new);
    SUB_COMMANDS.put("add", AddCommand::new);
    SUB_COMMANDS.put("remove", RemoveCommand::new);
  }

  private Context mContext;
  private AlluxioConfiguration mConf;

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public PathConfCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mContext = context;
    mConf = alluxioConf;
  }

  @Override
  public String getCommandName() {
    return "pathConf";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }

  @Override
  public Options getOptions() {
    Options options = new Options();
    SUB_COMMANDS.forEach((cmd, constructor) ->
        constructor.apply(mContext, mConf).getOptions().getOptions().forEach(
            option -> options.addOption(option)));
    return options;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    if (!SUB_COMMANDS.containsKey(args[0])) {
      throw new InvalidArgumentException("Unknown sub-command: " + args[0]);
    }
    Command cmd = SUB_COMMANDS.get(args[0]).apply(mContext, mConf);
    cmd.run(cl);
    return 0;
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
    return "Manage path level configuration, see sub-commands' descriptions for more details.";
  }
}
