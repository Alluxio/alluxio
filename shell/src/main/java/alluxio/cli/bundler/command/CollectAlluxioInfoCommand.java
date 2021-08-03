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
    /**
     * Creates an instance of {@link AlluxioCommand}.
     *
     * @param alluxioPath where Alluxio can be found
     * @param cmd         Alluxio cmd to run
     */
    public AlluxioCommand(String alluxioPath, String cmd) {
      super((alluxioPath + " " + cmd).split(" "));
    }
  }

  @Override
  protected void registerCommands() {
    // TODO(jiacheng): a command to find lost blocks?
    // alluxio getConf will mask the credential fields
    registerCommand("getConf",
        new AlluxioCommand(mAlluxioPath, "getConf"), null);
    registerCommand("getConf master",
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
        getListJournalCommand());
    registerCommand("runTests",
        new AlluxioCommand(mAlluxioPath, "runTests"), null);
    registerCommand("validateConf",
        new AlluxioCommand(mAlluxioPath, "validateConf"), null);
  }

  /**
   * Determine how to list the journal based on the type of UFS.
   * TODO(jiacheng): phase 2 support smarter detection
   * */
  private ShellCommand getListJournalCommand() {
    String journalPath = mFsContext.getClusterConf().get(PropertyKey.MASTER_JOURNAL_FOLDER);
    if (journalPath.startsWith("hdfs:")) {
      return new ShellCommand(new String[]{"hdfs", "dfs", "-ls", "-R", journalPath});
    } else {
      return new ShellCommand(new String[]{"ls", "-al", "-R", journalPath});
    }
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
