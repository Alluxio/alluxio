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
import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.Configuration;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.predicate.FilePredicate;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobFactory;
import alluxio.scheduler.job.JobState;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.base.Predicates;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Predicate;

/**
 * Factory for creating {@link LoadJob}s from journal entries.
 */
@SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "Field will be used in future")
public class JournalLoadJobFactory implements JobFactory {
  private final DefaultFileSystemMaster mFsMaster;

  private final alluxio.proto.journal.Job.LoadJobEntry mJobEntry;

  /**
   * Create factory.
   * @param journalEntry journal entry
   * @param fsMaster file system master
   */
  public JournalLoadJobFactory(alluxio.proto.journal.Job.LoadJobEntry journalEntry,
       DefaultFileSystemMaster fsMaster) {
    mFsMaster = fsMaster;
    mJobEntry = journalEntry;
  }

  @Override
  public Job<?> create() {
    String path = mJobEntry.getLoadPath();
    UnderFileSystem ufs = mFsMaster.getUfsManager().getOrAdd(new AlluxioURI(path),
        () -> UnderFileSystemConfiguration.defaults(Configuration.global()));
    Predicate<UfsStatus> predicate = Predicates.alwaysTrue();
    Optional<String> fileFilterRegx = Optional.empty();
    if (mJobEntry.hasFileFilterRegx()) {
      String regxPatternStr = mJobEntry.getFileFilterRegx();
      if (regxPatternStr != null && !regxPatternStr.isEmpty()) {
        alluxio.proto.journal.Job.FileFilter.Builder builder =
            alluxio.proto.journal.Job.FileFilter.newBuilder()
                .setName("fileNamePattern").setValue(regxPatternStr);
        FilePredicate filePredicate = FilePredicate.create(builder.build());
        predicate = filePredicate.getUfsStatusPredicate();
        fileFilterRegx = Optional.of(regxPatternStr);
      }
    }
    Iterable<UfsStatus> iterable = new UfsStatusIterable(ufs, path,
        Optional.ofNullable(AuthenticatedClientUser.getOrNull()).map(User::getName),
        predicate);
    Optional<String> user =
        mJobEntry.hasUser() ? Optional.of(mJobEntry.getUser()) : Optional.empty();
    DoraLoadJob job = new DoraLoadJob(path, user, mJobEntry.getJobId(),
        mJobEntry.hasBandwidth() ? OptionalLong.of(mJobEntry.getBandwidth()) : OptionalLong.empty(),
        mJobEntry.getPartialListing(), mJobEntry.getVerify(), mJobEntry.getLoadMetadataOnly(),
        mJobEntry.getSkipIfExists(), fileFilterRegx, iterable.iterator(), ufs, 1);
    job.setJobState(JobState.fromProto(mJobEntry.getState()), false);
    if (mJobEntry.hasEndTime()) {
      job.setEndTime(mJobEntry.getEndTime());
    }
    return job;
  }
}
