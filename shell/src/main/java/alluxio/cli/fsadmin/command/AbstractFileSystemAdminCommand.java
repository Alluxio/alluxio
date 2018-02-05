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

package alluxio.cli.fsadmin.command;

import alluxio.cli.AbstractCommand;
import alluxio.client.file.FileSystemMasterClient;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for all the FileSystemAdmin {@link alluxio.cli.Command} classes.
 * It provides a place to hold the {@link FileSystemMasterClient} client.
 */
@ThreadSafe
public abstract class AbstractFileSystemAdminCommand extends AbstractCommand {

  protected FileSystemMasterClient mMasterClient;

  protected AbstractFileSystemAdminCommand(FileSystemMasterClient masterClient) {
    mMasterClient = masterClient;
  }
}
