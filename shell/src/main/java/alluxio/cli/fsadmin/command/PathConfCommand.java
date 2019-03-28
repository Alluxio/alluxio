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
import alluxio.cli.fsadmin.pathconf.ListCommand;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.CommonUtils;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages path level configuration.
 */
public final class PathConfCommand extends AbstractFsAdminCommand {
  private enum SubCommand {
    LIST("list", ListCommand.class),
    ;

    private final String mName;
    private final Class<? extends Command> mClass;

    /**
     * @param name the sub-command's name
     * @param cmdClass the sub-command's class
     */
    SubCommand(String name, Class<? extends Command> cmdClass) {
      mName = name;
      mClass = cmdClass;
    }

    /**
     * @return the sub-command's name
     */
    public String getName() {
      return mName;
    }

    /**
     * @return the sub-command's class
     */
    public Class<? extends Command> getCommandClass() {
      return mClass;
    }
  }

  /** A map from sub-commands' names to the definition. */
  private static final Map<String, SubCommand> SUB_COMMAND_MAP = new HashMap<>();

  static {
    for (SubCommand cmd : SubCommand.values()) {
      SUB_COMMAND_MAP.put(cmd.getName(), cmd);
    }
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
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    if (!SUB_COMMAND_MAP.containsKey(args[0])) {
      throw new InvalidArgumentException("Unknown sub-command: " + args[0]);
    }
    SubCommand subCmd = SUB_COMMAND_MAP.get(args[0]);
    Command cmd = CommonUtils.createNewClassInstance(subCmd.getCommandClass(),
        new Class<?>[]{Context.class, AlluxioConfiguration.class},
        new Object[]{mContext, mConf});
    cmd.run(cl);
    return 0;
  }

  @Override
  public String getUsage() {
    StringBuilder usage = new StringBuilder(getCommandName());
    for (SubCommand cmd : SubCommand.values()) {
      usage.append(" [").append(cmd.getName()).append("]");
    }
    return usage.toString();
  }

  @Override
  public String getDescription() {
    return "Manage path level configuration, see sub-commands' descriptions for more details.";
  }
}
