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

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobFactory;
import alluxio.scheduler.job.JobState;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.wire.FileInfo;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Factory for creating {@link MoveJob}s from journal entries.
 */
public class JournalMoveJobFactory implements JobFactory {
  private final DefaultFileSystemMaster mFs;

  private final alluxio.proto.journal.Job.MoveJobEntry mJobEntry;

  /**
   * Create factory.
   * @param journalEntry journal entry
   * @param fs file system master
   */
  public JournalMoveJobFactory(alluxio.proto.journal.Job.MoveJobEntry journalEntry,
                               DefaultFileSystemMaster fs) {
    mFs = fs;
    mJobEntry = journalEntry;
  }

  @Override
  public Job<?> create() {
    Optional<String> user =
        mJobEntry.hasUser() ? Optional.of(mJobEntry.getUser()) : Optional.empty();
    UnderFileSystem ufs = mFs.getUfsManager().getOrAdd(new AlluxioURI(mJobEntry.getSrc()),
        UnderFileSystemConfiguration.defaults(Configuration.global()));
    Iterable<FileInfo> fileIterator =
        new UfsFileIterable(ufs, mJobEntry.getSrc(), user, mJobEntry.getPartialListing(),
            FileInfo::isCompleted);
    AbstractJob<?> job = getMoveJob(user, fileIterator);
    job.setJobState(JobState.fromProto(mJobEntry.getState()), false);
    if (mJobEntry.hasEndTime()) {
      job.setEndTime(mJobEntry.getEndTime());
    }
    return job;
  }

  private MoveJob getMoveJob(Optional<String> user, Iterable<FileInfo> fileIterator) {
    MoveJob job =
        new MoveJob(mJobEntry.getSrc(), mJobEntry.getDst(), mJobEntry.getOverwrite(), user,
            mJobEntry.getJobId(),
            mJobEntry.hasBandwidth() ? OptionalLong.of(mJobEntry.getBandwidth()) :
                OptionalLong.empty(), mJobEntry.getPartialListing(), mJobEntry.getVerify(),
            mJobEntry.getCheckContent(), fileIterator);
    return job;
  }
}
