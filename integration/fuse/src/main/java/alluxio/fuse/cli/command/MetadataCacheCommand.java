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

package alluxio.fuse.cli.command;

import alluxio.client.file.FileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.fuse.cli.FuseCommand;
import alluxio.fuse.cli.metadatacache.DropAllCommand;
import alluxio.fuse.cli.metadatacache.DropCommand;
import alluxio.fuse.cli.metadatacache.SizeCommand;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Gets information of or makes changes to the Fuse client metadata cache.
 */
@ThreadSafe
public final class MetadataCacheCommand extends AbstractFuseShellCommand {

  private static final Map<String, BiFunction<FileSystem, AlluxioConfiguration,
      ? extends FuseCommand>> SUB_COMMANDS = new HashMap<>();

  static {
    SUB_COMMANDS.put("dropAll", DropAllCommand::new);
    SUB_COMMANDS.put("drop", DropCommand::new);
    SUB_COMMANDS.put("size", SizeCommand::new);
  }

  private Map<String, FuseCommand> mSubCommands = new HashMap<>();

  /**
   * @param fs filesystem instance from fuse command
   * @param conf configuration instance from fuse command
   */
  public MetadataCacheCommand(FileSystem fs, AlluxioConfiguration conf) {
    super(fs, conf);
    SUB_COMMANDS.forEach((name, constructor) -> {
      mSubCommands.put(name, constructor.apply(fs, conf));
    });
  }

  @Override
  public boolean hasSubCommand() {
    return true;
  }

  @Override
  public Map<String, FuseCommand> getSubCommands() {
    return mSubCommands;
  }

  @Override
  public String getCommandName() {
    return "metadatacache";
  }

  @Override
  public String getUsage() {
    // Show usage: metadatacache.([drop] [size] [dropAll])
    StringBuilder usage = new StringBuilder(getCommandName());
    usage.append(".(");
    for (String cmd : SUB_COMMANDS.keySet()) {
      usage.append("[").append(cmd).append("]");
    }
    usage.append(")");
    return usage.toString();
  }

  /*
  * @return command's description
  * */
  public static String description() {
    return "Metadatacache command is used to drop metadata cache or get cache size. ";
  }

  @Override
  public String getDescription() {
    return description();
  }
}
