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

package alluxio.cli.fsadmin.command;

import alluxio.client.MetaMasterClient;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystemMasterClient;

import com.google.common.base.Preconditions;

import java.io.PrintStream;

/**
 * Context for running an fsadmin command.
 */
public final class Context {
  private final FileSystemMasterClient mFsClient;
  private final BlockMasterClient mBlockClient;
  private final MetaMasterClient mMetaClient;
  private final PrintStream mPrintStream;

  /**
   * @param fsClient filesystem master client
   * @param blockClient block master client
   * @param metaClient meta master client
   * @param printStream print stream to write to
   */
  public Context(FileSystemMasterClient fsClient, BlockMasterClient blockClient,
      MetaMasterClient metaClient, PrintStream printStream) {
    mFsClient = Preconditions.checkNotNull(fsClient, "fsClient");
    mBlockClient = Preconditions.checkNotNull(blockClient, "blockClient");
    mMetaClient = Preconditions.checkNotNull(metaClient, "metaClient");
    mPrintStream = Preconditions.checkNotNull(printStream, "printStream");
  }

  /**
   * @return the filesystem master client
   */
  public FileSystemMasterClient getFsClient() {
    return mFsClient;
  }

  /**
   * @return the block master client
   */
  public BlockMasterClient getBlockClient() {
    return mBlockClient;
  }

  /**
   * @return the meta master client
   */
  public MetaMasterClient getMetaClient() {
    return mMetaClient;
  }

  /**
   * @return the print stream to write to
   */
  public PrintStream getPrintStream() {
    return mPrintStream;
  }
}
