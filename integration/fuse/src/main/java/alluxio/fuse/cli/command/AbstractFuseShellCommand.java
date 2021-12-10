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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for all the Fuse {@link alluxio.fuse.cli.command} classes.
 */
@ThreadSafe
public abstract class AbstractFuseShellCommand implements FuseCommand {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFuseShellCommand.class);
  protected final AlluxioConfiguration mConf;
  protected final FileSystem mFileSystem;
  protected final String mParentCommandName;

  public AbstractFuseShellCommand(FileSystem fileSystem,
      AlluxioConfiguration alluxioConfiguration, String commandName) {
    mFileSystem = fileSystem;
    mConf = alluxioConfiguration;
    mParentCommandName = commandName;
  }

  /*
  * Get the parent command name, if parent command doesn't exist, return null
  */
  public String getParentCommandName() {
    return mParentCommandName;
  }
}
