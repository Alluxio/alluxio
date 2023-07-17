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
import alluxio.exception.runtime.FailedPreconditionRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.meta.MountTable;
import alluxio.master.predicate.FilePredicate;
import alluxio.proto.journal.Job.FileFilter;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobFactory;
import alluxio.scheduler.job.JobState;
import alluxio.underfs.UnderFileSystem;
import alluxio.wire.FileInfo;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Predicate;

/**
 * Factory for creating {@link LoadJob}s from journal entries.
 */
public class JournalCopyJobFactory implements JobFactory {

  private final DefaultFileSystemMaster mFs;

  private final alluxio.proto.journal.Job.CopyJobEntry mJobEntry;

  /**
   * Create factory.
   * @param journalEntry journal entry
   * @param fs file system master
   */
  public JournalCopyJobFactory(alluxio.proto.journal.Job.CopyJobEntry journalEntry,
       DefaultFileSystemMaster fs) {
    mFs = fs;
    mJobEntry = journalEntry;
  }

  @Override
  public Job<?> create() {
    Optional<String> user =
        mJobEntry.hasUser() ? Optional.of(mJobEntry.getUser()) : Optional.empty();
    MountTable.ReverseResolution resolution =
        mFs.getMountTable().reverseResolve(new AlluxioURI(mJobEntry.getSrc()));
    long mountId;
    if (resolution == null) {
      throw new NotFoundRuntimeException("Mount point not found");
    }
    else {
      mountId = resolution.getMountInfo().getMountId();
    }
    UnderFileSystem ufs;
    try {
      ufs = mFs.getUfsManager().get(mountId).acquireUfsResource().get();
    } catch (NotFoundException | UnavailableException e) {
      // concurrent mount table change would cause this exception
      throw new FailedPreconditionRuntimeException(e);
    }
    Predicate<FileInfo> predicate = mJobEntry.hasFilter() ? FilePredicate
        .create(mJobEntry.getFilter()).get() : FileInfo::isCompleted;
    Iterable<FileInfo> fileIterator =
        new UfsFileIterable(ufs, mJobEntry.getSrc(), user, predicate);
    AbstractJob<?> job = getCopyJob(user, fileIterator);
    job.setJobState(JobState.fromProto(mJobEntry.getState()), false);
    if (mJobEntry.hasEndTime()) {
      job.setEndTime(mJobEntry.getEndTime());
    }
    return job;
  }

  private CopyJob getCopyJob(Optional<String> user, Iterable<FileInfo> fileIterator) {
    Optional<FileFilter> fileFilter = mJobEntry.hasFilter() ? Optional.of(mJobEntry.getFilter()) :
        Optional.empty();
    CopyJob job =
        new CopyJob(mJobEntry.getSrc(), mJobEntry.getDst(), mJobEntry.getOverwrite(), user,
            mJobEntry.getJobId(),
            mJobEntry.hasBandwidth() ? OptionalLong.of(mJobEntry.getBandwidth()) :
                OptionalLong.empty(), mJobEntry.getPartialListing(), mJobEntry.getVerify(),
            mJobEntry.getCheckContent(), fileIterator, fileFilter);
    return job;
  }
}

