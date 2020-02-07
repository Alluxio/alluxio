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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.ConfigurationUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class for collecting various information about the host instance.
 */
public class CollectInfo extends AbstractShell {
  protected static final String TARBALL_NAME = "alluxio-info.tar.gz";

  private static final Map<String, String[]> CMD_ALIAS = ImmutableMap.of();

  // In order for a warning to be displayed for an unstable alias, it must also exist within the
  // CMD_ALIAS map.
  private static final Set<String> UNSTABLE_ALIAS = ImmutableSet.of();

  /**
   * Main method, starts a new CollectInfo.
   *
   * @param argv array of arguments given by the user's input from the terminal
   */
  public static void main(String[] argv) throws IOException, InvalidArgumentException {
    int ret = 0;

    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // Execute the Collectors one by one
    // Reduce the RPC retry max duration to fall earlier for CLIs
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s", Source.DEFAULT);
    CollectInfo shell = new CollectInfo(conf);

    // If the args are not valid, return early
    // TODO(jiacheng): catch in CollectInfoAll
    if (argv.length < 2) {
      shell.printUsage();
      throw new InvalidArgumentException(ExceptionMessage.INVALID_ARGS_NUM_INSUFFICIENT
              .getMessage("collectInfo", 2, argv.length));
    }

    // Determine the command and working dir path
    String subCommand = argv[0];
    // TODO(jiacheng): phase 2 get targetDirPath from parsed command
    String targetDirPath = argv[1];

    // There are 2 cases:
    // 1. Execute "all" commands
    // 2. Execute a single command
    List<File> filesToCollect = new ArrayList<>();
    if (subCommand.equals("all")) {
      // Case 1. Execute "all" commands
      System.out.println("Execute all child commands");
      for (Command cmd : shell.getCommands()) {
        System.out.format("Executing %s", cmd.getCommandName());

        // Replace the action with the command to execute
        // TODO(jiacheng): phase 2 handle argv difference?
        String[] replacementArgv =
                (String[]) ArrayUtils.addAll(new String[]{cmd.getCommandName()},
                        ArrayUtils.subarray(argv, 1, argv.length));
        int childRet = shell.executeAndAddFile(replacementArgv, filesToCollect);

        // If any of the commands failed, treat as failed
        if (ret == 0 && childRet != 0) {
          ret = childRet;
        }
      }
    } else {
      // Case 2. Execute a single command
      int childRet = shell.executeAndAddFile(argv, filesToCollect);
      if (ret == 0 && childRet != 0) {
        ret = childRet;
      }
    }

    // TODO(jiacheng): phase 2 add an option to disable bundle
    // Generate bundle
    System.out.format("Archiving dir %s", targetDirPath);
    String tarballPath = Paths.get(targetDirPath, TARBALL_NAME).toAbsolutePath().toString();
    if (filesToCollect.size() == 0) {
      System.err.format("No files to add. Tarball %s will be empty!", tarballPath);
    }
    TarUtils.compress(tarballPath, filesToCollect.toArray(new File[0]));
    System.out.println("Archiving finished");

    System.exit(ret);
  }

  private int executeAndAddFile(String[] argv, List<File> filesToCollect) throws IOException {
    // The argv length has been validated
    String subCommand = argv[0];
    // TODO(jiacheng): phase 2 get targetDirPath from parsed command
    String targetDirPath = argv[1];

    AbstractInfoCollectorCommand cmd = this.findCommand(subCommand);

    if (cmd == null) {
      // Unknown command (we did not find the cmd in our dict)
      System.err.format("%s is an unknown command.", cmd);
      printUsage();
      return 1;
    }
    int ret = run(argv);

    // File to collect
    File infoCmdOutputFile = cmd.generateOutputFile(targetDirPath,
            cmd.getCommandName());
    filesToCollect.add(infoCmdOutputFile);

    return ret;
  }

  private AbstractInfoCollectorCommand findCommand(String cmdName) {
    for (Command c : this.getCommands()) {
      if (c.getCommandName().equals(cmdName)) {
        return (AbstractInfoCollectorCommand) c;
      }
    }
    return null;
  }

  /**
   * Creates a new instance of {@link CollectInfo}.
   *
   * @param alluxioConf Alluxio configuration
   */
  public CollectInfo(InstancedConfiguration alluxioConf) {
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
            CollectInfo.class.getPackage().getName(),
            new Class[] {FileSystemContext.class},
            new Object[] {FileSystemContext.create(mConfiguration)});
    return commands;
  }
}
