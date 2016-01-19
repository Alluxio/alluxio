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

import tachyon.TachyonURI;
import tachyon.client.block.TachyonBlockStore;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;

/**
 * Displays the file's all blocks info.
 */
public final class FileInfoCommand extends WithWildCardPathCommand {

  /**
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public FileInfoCommand(TachyonConf conf, TachyonFileSystem tfs) {
    super(conf, tfs);
  }

  @Override
  public String getCommandName() {
    return "fileInfo";
  }

  @Override
  void runCommand(TachyonURI path) throws IOException {
    TachyonFile fd;
    FileInfo fInfo;
    try {
      fd = mTfs.open(path);
      fInfo = mTfs.getInfo(fd);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }

    if (fInfo.isIsFolder()) {
      throw new IOException(path + " is a directory path so does not have file blocks.");
    }

    System.out.println(fInfo);
    System.out.println("Containing the following blocks: ");
    for (long blockId : fInfo.getBlockIds()) {
      System.out.println(TachyonBlockStore.get().getInfo(blockId));
    }
  }

  @Override
  public String getUsage() {
    return "fileInfo <path>";
  }

  @Override
  public String getDescription() {
    return "Displays all block info for the specified file.";
  }
}
