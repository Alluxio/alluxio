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

import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobFactory;
import alluxio.scheduler.job.JobState;
import alluxio.underfs.UnderFileSystem;
import alluxio.wire.FileInfo;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Factory for creating {@link LoadJob}s from journal entries.
 */
public class JournalCopyJobFactory implements JobFactory {

  private final UnderFileSystem mFs;

  private final alluxio.proto.journal.Job.CopyJobEntry mJobEntry;

  /**
   * Create factory.
   * @param journalEntry journal entry
   * @param fs file system master
   */
  public JournalCopyJobFactory(alluxio.proto.journal.Job.CopyJobEntry journalEntry,
       UnderFileSystem fs) {
    mFs = fs;
    mJobEntry = journalEntry;
  }

  @Override
  public Job<?> create() {
    Optional<String> user =
        mJobEntry.hasUser() ? Optional.of(mJobEntry.getUser()) : Optional.empty();
    Iterable<FileInfo> fileIterator =
        new UfsFileIterable(mFs, mJobEntry.getSrc(), user, mJobEntry.getPartialListing(),
            FileInfo::isCompleted);
    AbstractJob<?> job = getCopyJob(user, fileIterator);
    job.setJobState(JobState.fromProto(mJobEntry.getState()));
    if (mJobEntry.hasEndTime()) {
      job.setEndTime(mJobEntry.getEndTime());
    }
    return job;
  }

  private CopyJob getCopyJob(Optional<String> user, Iterable<FileInfo> fileIterator) {
    CopyJob job =
        new CopyJob(mJobEntry.getSrc(), mJobEntry.getDst(), mJobEntry.getOverwrite(), user,
            mJobEntry.getJobId(),
            mJobEntry.hasBandwidth() ? OptionalLong.of(mJobEntry.getBandwidth()) :
                OptionalLong.empty(), mJobEntry.getPartialListing(), mJobEntry.getVerify(),
            fileIterator);
    return job;
  }
}

