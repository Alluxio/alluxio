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

import tachyon.client.block.TachyonBlockStore;
import tachyon.client.file.FileSystem;
import tachyon.conf.TachyonConf;

/**
 * Gets number of bytes used in the {@link FileSystem}.
 */
public final class GetUsedBytesCommand extends AbstractTfsShellCommand {

  /**
   * Constructs a new instance to get the number of bytes used in the {@link TachyonFileSystem}.
   *
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public GetUsedBytesCommand(TachyonConf conf, FileSystem tfs) {
    super(conf, tfs);
  }

  @Override
  public String getCommandName() {
    return "getUsedBytes";
  }

  @Override
  protected int getNumOfArgs() {
    return 0;
  }

  @Override
  public void run(String... args) throws IOException {
    long usedBytes = TachyonBlockStore.get().getUsedBytes();
    System.out.println("Used Bytes: " + usedBytes);
  }

  @Override
  public String getUsage() {
    return "getUsedBytes";
  }

  @Override
  public String getDescription() {
    return "Gets number of bytes used in the Tachyon file system.";
  }
}
