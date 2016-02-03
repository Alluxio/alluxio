/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.shell.command;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.cli.CommandLine;

import tachyon.TachyonURI;
import tachyon.client.block.TachyonBlockStore;
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.wire.BlockLocation;

/**
 * Displays a list of hosts that have the file specified in args stored.
 */
@ThreadSafe
public final class LocationCommand extends WithWildCardPathCommand {

  /**
   * Constructs a new instance to display a list of hosts that have the file specified in args
   * stored.
   *
   * @param conf the configuration for Tachyon
   * @param fs the filesystem of Tachyon
   */
  public LocationCommand(TachyonConf conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "location";
  }

  @Override
  void runCommand(TachyonURI path, CommandLine cl) throws IOException {
    URIStatus status;
    try {
      status = mFileSystem.getStatus(path);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }

    System.out.println(path + " with file id " + status.getFileId() + " is on nodes: ");
    for (long blockId : status.getBlockIds()) {
      for (BlockLocation location : TachyonBlockStore.get().getInfo(blockId).getLocations()) {
        System.out.println(location.getWorkerAddress().getHost());
      }
    }
  }

  @Override
  public String getUsage() {
    return "location <path>";
  }

  @Override
  public String getDescription() {
    return "Displays the list of hosts storing the specified file.";
  }
}
