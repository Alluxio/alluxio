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

package alluxio.master.job.tracker;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.job.wire.Status;
import alluxio.master.job.JobMaster;
import alluxio.util.CommonUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * The base class for all the distributed job runner classes.
 * It provides handling for submitting multiple jobs and handling retries of them.
 */
public abstract class AbstractCmdRunner {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractCmdRunner.class);
  protected static final int DEFAULT_ACTIVE_JOBS = 3000;
  protected static final long DEFAULT_FILE_COUNT = 1;

  protected FileSystem mFileSystem;
  protected FileSystemContext mFsContext;
  protected List<CmdRunAttempt> mSubmitted;
  protected Map<Long, List<CmdRunAttempt>> mJobMap;
  protected int mActiveJobs;
  protected final JobMaster mJobMaster;
  private int mFailedCount;
  private int mCompletedCount;

  // The FilesystemContext contains configuration information and is also used to instantiate a
  // filesystem client, if null - load default properties
  protected AbstractCmdRunner(@Nullable FileSystemContext fsContext, JobMaster jobMaster) {
    mSubmitted = Lists.newArrayList();
    mJobMap = Maps.newHashMap();
    if (fsContext == null) {
      fsContext = FileSystemContext.create();
    }
    mFsContext = fsContext;
    mFileSystem = FileSystem.Factory.create(fsContext);

    //final ClientContext clientContext = mFsContext.getClientContext();
    mActiveJobs = DEFAULT_ACTIVE_JOBS;
    mFailedCount = 0;
    mCompletedCount = 0;
    mJobMaster = jobMaster;
  }

  /**
   * Waits for at least one job to complete.
   */
  protected void waitForCmdJob() {
    AtomicBoolean removed = new AtomicBoolean(false);
    while (true) {
      mSubmitted = mSubmitted.stream().filter((attempt) -> {
        Status check = attempt.checkJobStatus();
        switch (check) {
          case CREATED:
          case RUNNING:
            return true;
          case CANCELED:
          case COMPLETED:
            mCompletedCount++;
            removed.set(true);
            return false;
          case FAILED:
            mFailedCount++;
            removed.set(true);
            return false;
          default:
            throw new IllegalStateException(String.format("Unexpected Status: %s", check));
        }
      }).collect(Collectors.toList());
      if (removed.get()) {
        return;
      }
      CommonUtils.sleepMs(5);
    }
  }
}
