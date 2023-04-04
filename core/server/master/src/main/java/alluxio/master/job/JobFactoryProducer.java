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

import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.job.CopyJobRequest;
import alluxio.job.JobRequest;
import alluxio.job.LoadJobRequest;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.proto.journal.Journal;
import alluxio.scheduler.job.JobFactory;
import alluxio.underfs.UnderFileSystem;
import alluxio.wire.MountPointInfo;

import java.util.Map;

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
      return new LoadJobFactory((LoadJobRequest) request, fsMaster);
    }
    if (request instanceof CopyJobRequest) {
      CopyJobRequest copyRequest = (CopyJobRequest) request;
      Map<String, MountPointInfo> mountPointInfoSummary =
          fsMaster.getMountPointInfoSummary(false);
      UnderFileSystem ufsClient;
      // no need to close under file system cause most close methods are noop
      try {
        ufsClient = fsMaster.getUfsManager()
            .get(mountPointInfoSummary.get(copyRequest.getSrcUfsAddress()).getMountId())
            .acquireUfsResource().get();
      } catch (NotFoundException | UnavailableException e) {
        throw AlluxioRuntimeException.from(e);
      }
      return new CopyJobFactory((CopyJobRequest) request, ufsClient);
    }
    throw new IllegalArgumentException("Unknown job type: " + request.getType());
  }

  /**
   * @param entry the job journal entry
   * @param fs    the file system master
   * @return the job factory
   */
  public static JobFactory create(Journal.JournalEntry entry, UnderFileSystem fs) {
    if (entry.hasCopyJob()) {
      return new JournalCopyJobFactory(entry.getCopyJob(), fs);
    }
    else {
      throw new IllegalArgumentException("Unknown job type: " + entry);
    }
  }

  /**
   * @param entry    the job journal entry
   * @param fsMaster the file system master
   * @return the job factory
   */
  public static JobFactory create(Journal.JournalEntry entry, FileSystemMaster fsMaster) {
    if (entry.hasLoadJob()) {
      return new JournalLoadJobFactory(entry.getLoadJob(), fsMaster);
    }
    else {
      throw new IllegalArgumentException("Unknown job type: " + entry);
    }
  }
}
