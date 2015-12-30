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
import tachyon.client.file.options.DeleteOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;

/**
 * Removes the file or directory specified by args. Will remove all files and directories in the
 * directory if a directory is specified.
 */
public final class RmrCommand extends WithWildCardPathCommand {

  public RmrCommand(TachyonConf conf, TachyonFileSystem tfs) {
    super(conf, tfs);
  }

  @Override
  public String getCommandName() {
    return "rmr";
  }

  @Override
  void runCommand(TachyonURI path) throws IOException {
    try {
      DeleteOptions options = new DeleteOptions.Builder().setRecursive(true).build();
      TachyonFile fd = mTfs.open(path);
      mTfs.delete(fd, options);
      System.out.println(path + " has been removed");
    } catch (InvalidPathException e) {
      System.out.println(path + " does not exist");
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "rmr <path>";
  }
}
