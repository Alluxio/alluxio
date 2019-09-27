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

package alluxio.cli.fs.command;

import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Gets the capacity of the {@link FileSystem}.
 */
@ThreadSafe
@PublicApi
public final class GetCapacityBytesCommand extends AbstractFileSystemCommand {
  /**
   * Constructs a new instance to get the capacity of the {@link FileSystem}.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public GetCapacityBytesCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "getCapacityBytes";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    AlluxioBlockStore alluxioBlockStore = AlluxioBlockStore.create(mFsContext);
    long capacityBytes = alluxioBlockStore.getCapacityBytes();
    System.out.println("Capacity Bytes: " + capacityBytes);
    return 0;
  }

  @Override
  public String getUsage() {
    return "getCapacityBytes";
  }

  @Override
  public String getDescription() {
    return "Gets the capacity of the Alluxio file system.";
  }
}
