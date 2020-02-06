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

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

/**
 * Command to collect Alluxio config files.
 * */
public class CollectConfigCommand extends AbstractInfoCollectorCommand {
  public static final String COMMAND_NAME = "collectConfig";
  private static final Logger LOG = LoggerFactory.getLogger(CollectConfigCommand.class);

  /**
   * Creates a new instance of {@link CollectConfigCommand}.
   *
   * @param fsContext the {@link FileSystemContext} to execute in
   * */
  public CollectConfigCommand(@Nullable FileSystemContext fsContext) {
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
    int ret = 0;

    // Determine the working dir path
    mWorkingDirPath = getWorkingDirectory(cl);

    String confDir = mFsContext.getClusterConf().get(PropertyKey.CONF_DIR);

    // TODO(jiacheng): Copy intelligently
    FileUtils.copyDirectory(new File(confDir), new File(mWorkingDirPath), true);

    return ret;
  }

  @Override
  public String getUsage() {
    return "collectConfig";
  }

  @Override
  public String getDescription() {
    return "Collect configurations";
  }
}
