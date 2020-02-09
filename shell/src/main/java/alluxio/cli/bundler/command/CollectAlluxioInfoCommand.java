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

import java.nio.file.Paths;

/**
 * Command to run a set of Alluxio commands.
 * Collects information about the Alluxio cluster.
 * */
public class CollectAlluxioInfoCommand extends ExecuteShellCollectInfoCommand {
  public static final String COMMAND_NAME = "collectAlluxioInfo";
  private static final Logger LOG = LoggerFactory.getLogger(CollectAlluxioInfoCommand.class);

  private String mAlluxioPath;

  /**
   * Creates a new instance of {@link CollectAlluxioInfoCommand}.
   *
   * @param fsContext the {@link FileSystemContext} to execute in
   * */
  public CollectAlluxioInfoCommand(FileSystemContext fsContext) {
    super(fsContext);
    mAlluxioPath = Paths.get(fsContext.getClusterConf().get(PropertyKey.WORK_DIR), "bin/alluxio")
            .toAbsolutePath().toString();
    registerCommands();
  }

  /**
   * A special shell command that runs an Alluxio cmdline operation.
   * */
  public static class AlluxioCommand extends ShellCommand {
    String mAlluxioPath;

    /**
     * Creates an instance of {@link AlluxioCommand}.
     *
     * @param alluxioPath where Alluxio can be found
     * @param cmd         Alluxio cmd to run
     */
    public AlluxioCommand(String alluxioPath, String cmd) {
      super(new String[]{alluxioPath, cmd});
      mAlluxioPath = alluxioPath;
    }
  }

  @Override
  protected void registerCommands() {
    // TODO(jiacheng): a command to find lost blocks?
    registerCommand("getConf",
            new AlluxioCommand(mAlluxioPath, "getConf --master --source"), null);
    registerCommand("fsadmin",
            new AlluxioCommand(mAlluxioPath, "fsadmin report"), null);
    registerCommand("mount",
            new AlluxioCommand(mAlluxioPath, "fs mount"), null);
    registerCommand("version",
            new AlluxioCommand(mAlluxioPath, "version -r"), null);
    registerCommand("job",
            new AlluxioCommand(mAlluxioPath, "job ls"), null);
    registerCommand("journal",
            new AlluxioCommand(mAlluxioPath, String.format("fs ls -R %s",
                    mFsContext.getClusterConf().get(PropertyKey.MASTER_JOURNAL_FOLDER))),
            new ShellCommand(new String[]{"ls", "-al", "-R",
                    mFsContext.getClusterConf().get(PropertyKey.MASTER_JOURNAL_FOLDER)}));
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

  @Override
  public String getUsage() {
    return "collectAlluxioInfo <outputPath>";
  }

  @Override
  public String getDescription() {
    return "Run a list of Alluxio commands to collect Alluxio cluster information";
  }
}
