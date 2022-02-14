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

import alluxio.cli.Command;
import alluxio.cli.command.metric.TotalCallsCommand;
import alluxio.client.file.FileSystem;
import alluxio.collections.TwoKeyConcurrentMap;
import alluxio.conf.AlluxioConfiguration;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Gets fuse related metrics.
 */
@ThreadSafe
public final class FuseMetricCommand extends AbstractFuseShellCommand {

  private static final Map<String, TwoKeyConcurrentMap.TriFunction<FileSystem,
      AlluxioConfiguration, String, ? extends Command>> SUB_COMMANDS = new HashMap<>();

  static {
    SUB_COMMANDS.put("totalCalls", TotalCallsCommand::new);
  }

  private final Map<String, Command> mSubCommands = new HashMap<>();

  /**
   * @param fs filesystem instance from fuse command
   * @param conf configuration instance from fuse command
   */
  public FuseMetricCommand(FileSystem fs, AlluxioConfiguration conf) {
    super(fs, conf, "");
    SUB_COMMANDS.forEach((name, constructor) -> {
      mSubCommands.put(name, constructor.apply(fs, conf, getCommandName()));
    });
  }

  @Override
  public Map<String, Command> getSubCommands() {
    return mSubCommands;
  }

  @Override
  public String getCommandName() {
    return "fusemetric";
  }

  @Override
  public String getDescription() {
    return "Fusemetric command is used to get some useful metric information.";
  }
}
