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
 * Gets the capacity of the {@link FileSystem}.
 */
public final class GetCapacityBytesCommand extends AbstractTfsShellCommand {

  /**
   * Constructs a new instance to get the capacity of the {@link TachyonFileSystem}.
   *
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public GetCapacityBytesCommand(TachyonConf conf, FileSystem tfs) {
    super(conf, tfs);
  }

  @Override
  public String getCommandName() {
    return "getCapacityBytes";
  }

  @Override
  protected int getNumOfArgs() {
    return 0;
  }

  @Override
  public void run(String... args) throws IOException {
    long capacityBytes = TachyonBlockStore.get().getCapacityBytes();
    System.out.println("Capacity Bytes: " + capacityBytes);
  }

  @Override
  public String getUsage() {
    return "getCapacityBytes";
  }

  @Override
  public String getDescription() {
    return "Gets the capacity of the Tachyon file system.";
  }
}
