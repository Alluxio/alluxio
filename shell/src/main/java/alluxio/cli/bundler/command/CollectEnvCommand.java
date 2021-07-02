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

package alluxio.cli.bundler.command;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.shell.ShellCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to run a set of bash commands to get system information.
 * */
public class CollectEnvCommand extends ExecuteShellCollectInfoCommand {
  public static final String COMMAND_NAME = "collectEnv";
  private static final Logger LOG = LoggerFactory.getLogger(CollectEnvCommand.class);

  /**
   * Creates a new instance of {@link CollectEnvCommand}.
   *
   * @param fsContext the {@link FileSystemContext} to execute in
   * */
  public CollectEnvCommand(FileSystemContext fsContext) {
    super(fsContext);
    registerCommands();
  }

  @Override
  protected void registerCommands() {
    registerCommand("Alluxio ps",
            new ShellCommand(new String[]{"bash", "-c", "ps", "-ef", "| grep alluxio"}), null);
    registerCommand("Spark ps",
            new ShellCommand(new String[]{"bash", "-c", "ps", "-ef", "| grep spark"}), null);
    registerCommand("Yarn ps",
            new ShellCommand(new String[]{"bash", "-c", "ps", "-ef", "| grep yarn"}), null);
    registerCommand("Hdfs ps",
            new ShellCommand(new String[]{"bash", "-c", "ps", "-ef", "| grep hdfs"}), null);
    registerCommand("Presto ps",
            new ShellCommand(new String[]{"bash", "-c", "ps", "-ef", "| grep presto"}), null);
    registerCommand("env",
            new ShellCommand(new String[]{"env"}), null);
    registerCommand("top", new ShellCommand(new String[]{"atop", "-b", "-n", "1"}),
            new ShellCommand(new String[]{"top", "-b", "-n", "1"}));
    registerCommand("mount", new ShellCommand(new String[]{"mount"}), null);
    registerCommand("df", new ShellCommand(new String[]{"df", "-H"}), null);
    registerCommand("ulimit", new ShellCommand(new String[]{"ulimit", "-Ha"}), null);
    registerCommand("uname", new ShellCommand(new String[]{"uname", "-a"}), null);
    registerCommand("hostname", new ShellCommand(new String[]{"hostname"}), null);
    registerCommand("host ip", new ShellCommand(new String[]{"hostname", "-i"}), null);
    registerCommand("host fqdn", new ShellCommand(new String[]{"hostname", "-f"}), null);
    registerCommand("list Alluxio home",
            new ShellCommand(new String[]{String.format("ls", "-al -R %s",
                    mFsContext.getClusterConf().get(PropertyKey.HOME))}), null);
    registerCommand("dig", new ShellCommand(new String[]{"dig", "$(hostname -i)"}), null);
    registerCommand("nslookup", new ShellCommand(new String[]{"nslookup", "$(hostname -i)"}), null);
    registerCommand("dstat", new ShellCommand(
            new String[]{"dstat", "-cdgilmnprsty", "1", "5"}), null);
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

  @Override
  public boolean hasSubCommand() {
    return false;
  }

  @Override
  public String getUsage() {
    return "collectEnv <outputPath>";
  }

  @Override
  public String getDescription() {
    return "Collect environment information by running a set of shell commands. ";
  }
}
