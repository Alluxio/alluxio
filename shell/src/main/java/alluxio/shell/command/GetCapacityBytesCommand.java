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

package alluxio.shell.command;

import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Gets the capacity of the {@link FileSystem}.
 */
@ThreadSafe
public final class GetCapacityBytesCommand extends AbstractShellCommand {

  /** The block store client. */
  private final AlluxioBlockStore mBlockStore;

  /**
   * Constructs a new instance to get the capacity of the {@link FileSystem}.
   *
   * @param fs the filesystem of Alluxio
   */
  public GetCapacityBytesCommand(FileSystem fs) {
    super(fs);
    mBlockStore = new AlluxioBlockStore();
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
  public void run(CommandLine cl) throws IOException {
    long capacityBytes = mBlockStore.getCapacityBytes();
    System.out.println("Capacity Bytes: " + capacityBytes);
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
