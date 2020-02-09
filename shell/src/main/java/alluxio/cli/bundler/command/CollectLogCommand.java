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
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Command to collect Alluxio logs.
 * */
public class CollectLogCommand  extends AbstractCollectInfoCommand {
  public static final String COMMAND_NAME = "collectLog";
  private static final Logger LOG = LoggerFactory.getLogger(CollectLogCommand.class);

  /**
   * Creates a new instance of {@link CollectLogCommand}.
   *
   * @param fsContext the {@link FileSystemContext} to execute in
   * */
  public CollectLogCommand(FileSystemContext fsContext) {
    super(fsContext);
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
  public int run(CommandLine cl) throws AlluxioException, IOException {
    // Determine the working dir path
    mWorkingDirPath = getWorkingDirectory(cl);
    String logDir = mFsContext.getClusterConf().get(PropertyKey.LOGS_DIR);

    // TODO(jiacheng): phase 2 Copy intelligently find security risks
    // TODO(jiacheng): phase 2 components option
    FileUtils.copyDirectory(new File(logDir), new File(mWorkingDirPath), true);

    return 0;
  }

  @Override
  public String getUsage() {
    return "collectLogs <outputPath>";
  }

  @Override
  public String getDescription() {
    return "Collect Alluxio log files";
  }
}
