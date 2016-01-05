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
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.FreeOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;

/**
 * Frees the given file or folder from Tachyon in-memory (recursively freeing all children if a
 * folder).
 */
public final class FreeCommand extends WithWildCardPathCommand {

  /**
   * Constructs a new instance to free the given file or folder from Tachyon.
   *
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public FreeCommand(TachyonConf conf, TachyonFileSystem tfs) {
    super(conf, tfs);
  }

  @Override
  public String getCommandName() {
    return "free";
  }

  @Override
  void runCommand(TachyonURI path) throws IOException {
    try {
      FreeOptions options = new FreeOptions.Builder().setRecursive(true).build();
      TachyonFile fd = mTfs.open(path);
      mTfs.free(fd, options);
      System.out.println(path + " was successfully freed from memory.");
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "free <file path|folder path>";
  }

  @Override
  public String getDescription() {
    return "Frees the given file or directory.";
  }
}
