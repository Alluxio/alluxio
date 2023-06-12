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

import alluxio.job.CopyJobRequest;
import alluxio.job.JobRequest;
import alluxio.job.LoadJobRequest;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.proto.journal.Journal;
import alluxio.scheduler.job.JobFactory;

/**
 * Producer for {@link JobFactory}.
 */
public class JobFactoryProducer {
  private JobFactoryProducer() {
  } // prevent instantiation

  /**
   * @param request  the job request
   * @param fsMaster the file system master
   * @return the job factory
   */
  public static JobFactory create(JobRequest request, DefaultFileSystemMaster fsMaster) {
    if (request instanceof LoadJobRequest) {
      return new LoadJobFactory((LoadJobRequest) request);
    }
    if (request instanceof CopyJobRequest) {
      return new CopyJobFactory((CopyJobRequest) request, fsMaster);
    }
    throw new IllegalArgumentException("Unknown job type: " + request.getType());
  }

  /**
   * @param entry    the job journal entry
   * @param fsMaster the file system master
   * @return the job factory
   */
  public static JobFactory create(Journal.JournalEntry entry, DefaultFileSystemMaster fsMaster) {
    if (entry.hasLoadJob()) {
      return new JournalLoadJobFactory(entry.getLoadJob(), fsMaster);
    }
    if (entry.hasCopyJob()) {
      return new JournalCopyJobFactory(entry.getCopyJob(), fsMaster);
    }
    if (entry.hasMoveJob()) {
      return new JournalMoveJobFactory(entry.getMoveJob(), fsMaster);
    }
    else {
      throw new IllegalArgumentException("Unknown job type: " + entry);
    }
  }
}
