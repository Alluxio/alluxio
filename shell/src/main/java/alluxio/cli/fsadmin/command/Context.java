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

import alluxio.client.journal.JournalMasterClient;
import alluxio.client.meta.MetaMasterClient;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.meta.MetaMasterConfigClient;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Context for running an fsadmin command.
 */
public final class Context implements Closeable {
  private final FileSystemMasterClient mFsClient;
  private final BlockMasterClient mBlockClient;
  private final MetaMasterClient mMetaClient;
  private final MetaMasterConfigClient mMetaConfigClient;
  private JournalMasterClient mMasterJournalMasterClient;
  private JournalMasterClient mJobMasterJournalMasterClient;
  private final PrintStream mPrintStream;
  private final Closer mCloser;

  /**
   * @param fsClient filesystem master client
   * @param blockClient block master client
   * @param metaClient meta master client
   * @param metaConfigClient meta configuration master client
   * @param masterJournalMasterClient journal master client for master
   * @param jobMasterJournalMasterClient journal master client for job_master
   * @param printStream print stream to write to
   */
  public Context(FileSystemMasterClient fsClient, BlockMasterClient blockClient,
      MetaMasterClient metaClient, MetaMasterConfigClient metaConfigClient,
      JournalMasterClient masterJournalMasterClient,
      JournalMasterClient jobMasterJournalMasterClient, PrintStream printStream) {
    mCloser = Closer.create();
    mCloser.register(
        mFsClient = Preconditions.checkNotNull(fsClient, "fsClient"));
    mCloser.register(
        mBlockClient = Preconditions.checkNotNull(blockClient, "blockClient"));
    mCloser.register(
        mMetaClient = Preconditions.checkNotNull(metaClient, "metaClient"));
    mCloser.register(
        mMetaConfigClient = Preconditions.checkNotNull(metaConfigClient, "metaConfigClient"));
    mCloser.register(mMasterJournalMasterClient =
        Preconditions.checkNotNull(masterJournalMasterClient, "masterJournalMasterClient"));
    mCloser.register(mJobMasterJournalMasterClient =
        Preconditions.checkNotNull(jobMasterJournalMasterClient, "jobMasterJournalMasterClient"));
    mCloser.register(
        mPrintStream = Preconditions.checkNotNull(printStream, "printStream"));
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
   * @return the meta master configuration client
   */
  public MetaMasterConfigClient getMetaConfigClient() {
    return mMetaConfigClient;
  }

  /**
   * @return the journal master client for master
   */
  public JournalMasterClient getJournalMasterClientForMaster() {
    return mMasterJournalMasterClient;
  }

  /**
   * @return the journal master client for master
   */
  public JournalMasterClient getJournalMasterClientForJobMaster() {
    return mJobMasterJournalMasterClient;
  }

  /**
   * @return the print stream to write to
   */
  public PrintStream getPrintStream() {
    return mPrintStream;
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
}
