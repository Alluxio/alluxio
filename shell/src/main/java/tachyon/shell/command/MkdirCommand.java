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

import tachyon.TachyonURI;
import tachyon.client.file.FileSystem;
import tachyon.client.file.options.CreateDirectoryOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;

/**
 * Creates a new directory specified by the path in args, including any parent folders that are
 * required. This command fails if a directory or file with the same path already exists.
 */
@ThreadSafe
public final class MkdirCommand extends AbstractTfsShellCommand {

  /**
   * Constructs a new instance to create a new directory.
   *
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public MkdirCommand(TachyonConf conf, FileSystem tfs) {
    super(conf, tfs);
  }

  @Override
  public String getCommandName() {
    return "mkdir";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public void run(String... args) throws IOException {
    for (String path : args) {
      TachyonURI inputPath = new TachyonURI(path);

      try {
        CreateDirectoryOptions options = CreateDirectoryOptions.defaults().setRecursive(true);
        mTfs.createDirectory(inputPath, options);
        System.out.println("Successfully created directory " + inputPath);
      } catch (TachyonException e) {
        throw new IOException(e.getMessage());
      }
    }
  }

  @Override
  public String getUsage() {
    return "mkdir <path1> [path2] ... [pathn]";
  }

  @Override
  public String getDescription() {
    return "Creates the specified directories, including any parent directories that are required.";
  }

  @Override
  public boolean validateArgs(String... args) {
    boolean valid = args.length >= getNumOfArgs();
    if (!valid) {
      System.out.println(getCommandName() + " takes " + getNumOfArgs() + " argument at least\n");
    }
    return valid;
  }
}
