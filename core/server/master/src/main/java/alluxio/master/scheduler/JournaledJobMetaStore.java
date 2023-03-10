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

package alluxio.master.scheduler;

import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.job.JobFactoryProducer;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableIterator;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobMetaStore;

import com.google.common.collect.Iterators;

import java.util.Set;

/**
 * A journaled job meta store.
 */
public class JournaledJobMetaStore implements JobMetaStore, Journaled {
  private final FileSystemMaster mFileSystemMaster;
  private final Set<Job<?>>
      mExistingJobs = new ConcurrentHashSet<>();

  /**
   *  Creates a new instance of {@link JournaledJobMetaStore}.
   * @param fileSystemMaster the file system master
   */
  public JournaledJobMetaStore(FileSystemMaster fileSystemMaster) {
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public CloseableIterator<Journal.JournalEntry> getJournalEntryIterator() {
    return CloseableIterator.noopCloseable(
        Iterators.transform(mExistingJobs.iterator(), Job::toJournalEntry));
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    if (!entry.hasLoadJob()) {
      return false;
    }
    Job<?> job = JobFactoryProducer
        .create(entry, mFileSystemMaster).create();
    mExistingJobs.add(job);
    return true;
  }

  @Override
  public void resetState() {
    mExistingJobs.clear();
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.SCHEDULER;
  }

  @Override
  public void updateJob(Job<?> job) {
    try (JournalContext context = mFileSystemMaster.createJournalContext()) {
      context.append(job.toJournalEntry());
      mExistingJobs.add(job);
    } catch (UnavailableException e) {
      throw new UnavailableRuntimeException(
          "There is an ongoing backup running, please submit later", e);
    }
  }

  @Override
  public Set<Job<?>> getJobs() {
    return mExistingJobs;
  }
}
