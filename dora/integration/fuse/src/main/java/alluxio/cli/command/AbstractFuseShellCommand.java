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

import alluxio.cli.FuseCommand;
import alluxio.client.file.FileSystem;
import alluxio.conf.AlluxioConfiguration;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for all the Fuse {@link alluxio.cli.FuseCommand} classes.
 */
@ThreadSafe
public abstract class AbstractFuseShellCommand implements FuseCommand {
  protected final AlluxioConfiguration mConf;
  protected final FileSystem mFileSystem;
  protected final String mParentCommandName;

  /**
   * @param fileSystem the file system the command takes effect on
   * @param conf the Alluxio configuration
   * @param commandName the parent command name
   */
  public AbstractFuseShellCommand(FileSystem fileSystem,
      AlluxioConfiguration conf, String commandName) {
    mConf = conf;
    mFileSystem = fileSystem;
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
}
