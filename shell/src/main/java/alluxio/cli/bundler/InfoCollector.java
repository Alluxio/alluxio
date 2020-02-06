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

package alluxio.cli.bundler;

import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.cli.bundler.command.AbstractInfoCollectorCommand;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.util.ConfigurationUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class for collecting various information about the host instance.
 */
public class InfoCollector extends AbstractShell {
  protected static final String TARBALL_NAME = "collectAll.tar.gz";

  private static final Map<String, String[]> CMD_ALIAS = ImmutableMap.<String, String[]>builder()
          .build();

  // In order for a warning to be displayed for an unstable alias, it must also exist within the
  // CMD_ALIAS map.
  private static final Set<String> UNSTABLE_ALIAS = ImmutableSet.<String>builder()
          .build();

  /**
   * Main method, starts a new InfoCollector.
   *
   * @param argv array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException {
    int ret = 0;

    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // Execute the Collectors one by one
    // Reduce the RPC retry max duration to fall earlier for CLIs
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s", Source.DEFAULT);
    InfoCollector shell = new InfoCollector(conf);

    // Determine the working dir path
    if (argv.length < 2) {
      throw new IOException(String.format("No target directory specified by args %s",
              Arrays.toString(argv)));
    }
    String subCommand = argv[0];
    // TODO(jiacheng): phase 2 get targetDirPath from parsed command
    String targetDirPath = argv[1];

    // Invoke all other commands to collect information
    // FORCE_OPTION will be propagated to child commands
    List<File> filesToCollect = new ArrayList<>();
    if (subCommand.equals("all")) {
      // Execute all commands if the option is "all"
      System.out.println("Execute all child commands");
      for (Command cmd : shell.getCommands()) {
        System.out.println(String.format("Executing %s", cmd.getCommandName()));

        AbstractInfoCollectorCommand infoCmd = (AbstractInfoCollectorCommand) cmd;

        // Find the argv for this command
        argv[0] = infoCmd.getCommandName();
        // TODO(jiacheng): phase 2 handle argv difference?
        int childRet = shell.run(argv);
        System.out.println(String.format("Command %s exit with code %s",
                cmd.getCommandName(), childRet));

        // File to collect
        File infoCmdOutputFile = infoCmd.generateOutputFile(targetDirPath,
                infoCmd.getCommandName());
        filesToCollect.add(infoCmdOutputFile);

        // If any of the commands failed, treat as failed
        if (childRet > ret) {
          ret = childRet;
        }
      }
    } else {
      AbstractInfoCollectorCommand cmd = shell.findCommand(subCommand);
      if (cmd == null) {
        // Unknown command (we did not find the cmd in our dict)
        System.err.println(String.format("%s is an unknown command.", cmd));
        shell.printUsage();
        return;
      }

      int childRet = shell.run(argv);
      System.out.println(String.format("Command %s exit with code %s", subCommand, childRet));

      File infoCmdOutputFile = cmd.generateOutputFile(targetDirPath, cmd.getCommandName());
      filesToCollect.add(infoCmdOutputFile);

      if (childRet > ret) {
        ret = childRet;
      }
    }

    // TODO(jiacheng): phase 2 add an option to disable bundle
    // Generate bundle
    System.out.println(String.format("Archiving dir %s", targetDirPath));
    System.out.println(String.format("Files to include: %s", filesToCollect));
    String tarballPath = Paths.get(targetDirPath, TARBALL_NAME).toAbsolutePath().toString();
    TarUtils.compress(tarballPath, filesToCollect.toArray(new File[0]));
    System.out.println("Archiving finished");

    System.exit(ret);
  }

  /**
   * Finds the {@link AbstractInfoCollectorCommand} given command name.
   *
   * @param cmdName command name
   * @return the command
   * */
  public AbstractInfoCollectorCommand findCommand(String cmdName) {
    for (Command c : this.getCommands()) {
      if (c.getCommandName().equals(cmdName)) {
        return (AbstractInfoCollectorCommand) c;
      }
    }
    return null;
  }

  /**
   * Creates a new instance of {@link InfoCollector}.
   *
   * @param alluxioConf Alluxio configuration
   */
  public InfoCollector(InstancedConfiguration alluxioConf) {
    super(CMD_ALIAS, UNSTABLE_ALIAS, alluxioConf);
  }

  @Override
  protected String getShellName() {
    return "infoBundle";
  }

  @Override
  protected Map<String, Command> loadCommands() {
    // Give each command the configuration
    Map<String, Command> commands = CommandUtils.loadCommands(
            InfoCollector.class.getPackage().getName(),
            new Class[] {FileSystemContext.class},
            new Object[] {FileSystemContext.create(mConfiguration)});
    return commands;
  }
}
