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

import alluxio.cli.Command;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.job.JobMasterClient;
import alluxio.client.journal.JournalMasterClient;
import alluxio.client.meta.MetaMasterClient;
import alluxio.client.meta.MetaMasterConfigClient;
import alluxio.client.metrics.MetricsMasterClient;

import java.io.PrintStream;

/**
 * Base class for fsadmin commands. It provides access to clients for talking to fs master, block
 * master, and meta master.
 */
public abstract class AbstractFsAdminCommand implements Command {
  protected final FileSystemMasterClient mFsClient;
  protected final BlockMasterClient mBlockClient;
  protected final MetaMasterClient mMetaClient;
  protected final MetaMasterConfigClient mMetaConfigClient;
  protected final MetricsMasterClient mMetricsClient;
  protected final PrintStream mPrintStream;
  protected final JournalMasterClient mMasterJournalMasterClient;
  protected final JournalMasterClient mJobMasterJournalMasterClient;
  protected final JobMasterClient mJobMasterClient;

  protected AbstractFsAdminCommand(Context context) {
    mFsClient = context.getFsClient();
    mBlockClient = context.getBlockClient();
    mMetaClient = context.getMetaClient();
    mMetaConfigClient = context.getMetaConfigClient();
    mMasterJournalMasterClient = context.getJournalMasterClientForMaster();
    mMetricsClient = context.getMetricsClient();
    mJobMasterJournalMasterClient = context.getJournalMasterClientForJobMaster();
    mJobMasterClient = context.getJobMasterClient();
    mPrintStream = context.getPrintStream();
  }
}
