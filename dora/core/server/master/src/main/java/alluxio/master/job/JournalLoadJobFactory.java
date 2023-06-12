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

package alluxio.master.job;

import alluxio.annotation.SuppressFBWarnings;
import alluxio.master.file.FileSystemMaster;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobFactory;
import alluxio.scheduler.job.JobState;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Factory for creating {@link LoadJob}s from journal entries.
 */
@SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "Field will be used in future")
public class JournalLoadJobFactory implements JobFactory {
  private final FileSystemMaster mFsMaster;

  private final alluxio.proto.journal.Job.LoadJobEntry mJobEntry;

  /**
   * Create factory.
   * @param journalEntry journal entry
   * @param fsMaster file system master
   */
  public JournalLoadJobFactory(alluxio.proto.journal.Job.LoadJobEntry journalEntry,
       FileSystemMaster fsMaster) {
    mFsMaster = fsMaster;
    mJobEntry = journalEntry;
  }

  @Override
  public Job<?> create() {
    Optional<String> user =
        mJobEntry.hasUser() ? Optional.of(mJobEntry.getUser()) : Optional.empty();
    DoraLoadJob job = new DoraLoadJob(mJobEntry.getLoadPath(), user, mJobEntry.getJobId(),
        mJobEntry.hasBandwidth() ? OptionalLong.of(mJobEntry.getBandwidth()) : OptionalLong.empty(),
        mJobEntry.getPartialListing(), mJobEntry.getVerify(), mJobEntry.getLoadMetadataOnly());
    job.setJobState(JobState.fromProto(mJobEntry.getState()), false);
    if (mJobEntry.hasEndTime()) {
      job.setEndTime(mJobEntry.getEndTime());
    }
    return job;
  }
}
