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
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;

/**
 * Unpins the given file or folder (recursively unpinning all children if a folder). Pinned files
 * are never evicted from memory, so this method will allow such files to be evicted.
 */
public final class UnpinCommand extends WithWildCardPathCommand {

  /**
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public UnpinCommand(TachyonConf conf, TachyonFileSystem tfs) {
    super(conf, tfs);
  }

  @Override
  public String getCommandName() {
    return "unpin";
  }

  @Override
  void runCommand(TachyonURI path) throws IOException {
    CommandUtils.setPinned(mTfs, path, false);
    System.out.println("File '" + path + "' was successfully unpinned.");
  }

  @Override
  public String getUsage() {
    return "unpin <path>";
  }

  @Override
  public String getDescription() {
    return "Unpins the given file or folder from memory (works recursively for a directory).";
  }
}
