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

import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.cli.FuseCommand;
import alluxio.fuse.AlluxioFuseFileSystemOpts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for all the Fuse {@link alluxio.cli.FuseCommand} classes.
 */
@ThreadSafe
public abstract class AbstractFuseShellCommand implements FuseCommand {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFuseShellCommand.class);
  protected AlluxioFuseFileSystemOpts mFuseFsOpts;
  protected final FileSystem mFileSystem;
  protected final String mParentCommandName;

  /**
   * @param fileSystem the file system the command takes effect on
   * @param fuseFsOpts the options for AlluxioFuse filesystem
   * @param commandName the parent command name
   */
  public AbstractFuseShellCommand(FileSystem fileSystem, AlluxioFuseFileSystemOpts fuseFsOpts,
      String commandName) {
    mFileSystem = fileSystem;
    mFuseFsOpts = fuseFsOpts;
    mParentCommandName = commandName;
  }

  /**
  * Gets the parent command name.
  *
  * @return parent command name
  */
  public String getParentCommandName() {
    return mParentCommandName;
  }

  /**
   * Get command usage.
   * @return the usage information
   */
  public String getUsage() {
    // Show usage: Command.(subcommand1|subcommand2|subcommand3)
    StringBuilder usage = new StringBuilder("ls -l " + Constants.DEAFULT_FUSE_MOUNT
        + Constants.ALLUXIO_CLI_PATH + "." + getCommandName() + ".(");
    for (String cmd : getSubCommands().keySet()) {
      usage.append(cmd).append("|");
    }
    usage.deleteCharAt(usage.length() - 1).append(')');
    return usage.toString();
  }
}
