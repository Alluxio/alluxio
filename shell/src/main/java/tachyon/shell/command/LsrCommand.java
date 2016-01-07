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
 * Displays information for all directories and files under the path specified in args
 * recursively.
 */
public final class LsrCommand extends AbstractLsCommand {

  /**
   * Constructs a new instance to display information for all directories and files under the path
   * specified in args recursively.
   *
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public LsrCommand(TachyonConf conf, TachyonFileSystem tfs) {
    super(conf, tfs);
  }

  @Override
  public String getCommandName() {
    return "lsr";
  }

  @Override
  void runCommand(TachyonURI path) throws IOException {
    ls(path, true);
  }

  @Override
  public String getUsage() {
    return "lsr <path>";
  }

  @Override
  public String getDescription() {
    return "Displays information for all files and directories under the specified "
      + "path recursively.";
  }
}
