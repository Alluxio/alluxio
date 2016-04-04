/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.perf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import alluxio.perf.fs.PerfFS;
import alluxio.perf.fs.AlluxioPerfFS;
import alluxio.perf.thrift.SlaveAlreadyRegisterException;
import alluxio.perf.thrift.SlaveNotRegisterException;

/**
 * Maintain the state of each slave.
 */
public class SlaveStatus {
  private final int mSlavesNum;

  private final List<String> mCleanupDirs;
  private final Set<String> mFailedSlaves;
  private final Set<String> mReadySlaves;
  private final Set<String> mSuccessSlaves;
  private final Set<String> mUnregisterSlaves;

  public SlaveStatus(int slavesNum, Set<String> allSlaves) {
    mSlavesNum = slavesNum;
    mUnregisterSlaves = allSlaves;
    mCleanupDirs = new ArrayList<String>(mSlavesNum);
    mFailedSlaves = new HashSet<String>();
    mReadySlaves = new HashSet<String>();
    mSuccessSlaves = new HashSet<String>();
  }

  /**
   * Check if all the slaves are ready.
   * 
   * @param slaveName the name of the slave who sent this request
   * @return true if all the slaves are ready to run
   * @throws SlaveNotRegisterException
   */
  public synchronized boolean allReady(String slaveName) throws SlaveNotRegisterException {
    if (mUnregisterSlaves.contains(slaveName)) {
      throw new SlaveNotRegisterException(slaveName + " not register");
    }
    return (mReadySlaves.size() == mSlavesNum);
  }

  /**
   * Check if all the slaves are registered.
   * 
   * @return true if all the slaves are registered
   */
  public synchronized boolean allRegistered() {
    return mUnregisterSlaves.isEmpty();
  }

  /**
   * Clean up those directories declared by the slaves.
   * 
   * @throws IOException
   */
  public void cleanup() throws IOException {
    PerfFS fs = AlluxioPerfFS.get();
    synchronized (this) {
      for (String dir : mCleanupDirs) {
        if (fs.exists(dir)) {
          fs.delete(dir, true);
        }
      }
    }
    fs.close();
  }

  /**
   * Check if the test can finish. One case is that all the slaves finished successfully. Another
   * case is that enough slaves failed so the test should abort.
   * 
   * @param failedThenAbort if true, the test will abort when enough slaves are failed
   * @param failedPercentage the percentage threshold of the failed slaves
   * @return 0 if not finished; -1 if the test should abort; 1 if all the slaves finished
   *         successfully
   */
  public synchronized int finished(boolean failedThenAbort, int failedPercentage) {
    int success;
    int failed;
    failed = mFailedSlaves.size();
    success = mSuccessSlaves.size();
    if (failedThenAbort && (failed > (failedPercentage * mSlavesNum / 100))) {
      return -1;
    }
    if (success + failed == mSlavesNum) {
      return 1;
    }
    return 0;
  }

  /**
   * Get the status of all the slaves about how many slaves are running, how many slaves finished
   * successfully and how many slaves are failed.
   * 
   * @param debug if true, the information is more detailed to show the state of each slave
   * @return the status of all the slaves
   */
  public synchronized String getFinishStatus(boolean debug) {
    StringBuffer sbStatus = new StringBuffer();
    int running;
    int success;
    int failed;
    failed = mFailedSlaves.size();
    success = mSuccessSlaves.size();
    running = mReadySlaves.size() - failed - success;
    sbStatus.append("Running: ").append(running).append(" slaves. Success: ").append(success)
        .append(" slaves. Failed: ").append(failed).append(" slaves.");
    if (debug) {
      StringBuffer sbRunningSlaves = new StringBuffer("Running:");
      StringBuffer sbSuccessSlaves = new StringBuffer("Success:");
      StringBuffer sbFailedSlaves = new StringBuffer("Failed:");
      for (String slave : mReadySlaves) {
        if (mFailedSlaves.contains(slave)) {
          sbFailedSlaves.append(" " + slave);
        } else if (mSuccessSlaves.contains(slave)) {
          sbSuccessSlaves.append(" " + slave);
        } else {
          sbRunningSlaves.append(" " + slave);
        }
      }
      sbStatus.append("\n\t").append(sbRunningSlaves).append("\n\t").append(sbSuccessSlaves)
          .append("\n\t").append(sbFailedSlaves).append("\n");
    }
    return sbStatus.toString();
  }

  public synchronized Set<String> getUnregisterSlaves() {
    return new HashSet<String>(mUnregisterSlaves);
  }

  /**
   * One slave finished. Update the status.
   * 
   * @param slaveName the name of the finished slave
   * @param success true if the slave finished successfully
   * @throws SlaveNotRegisterException
   */
  public synchronized void slaveFinish(String slaveName, boolean success)
      throws SlaveNotRegisterException {
    if (mUnregisterSlaves.contains(slaveName)) {
      throw new SlaveNotRegisterException(slaveName + " not register");
    }
    if (success && !mFailedSlaves.contains(slaveName)) {
      mSuccessSlaves.add(slaveName);
    } else {
      mFailedSlaves.add(slaveName);
    }
  }

  /**
   * One slave setup and is ready to run. Update the status.
   * 
   * @param slaveName the name of the ready slave
   * @param success true if the slave setup successfully
   * @throws SlaveNotRegisterException
   */
  public synchronized void slaveReady(String slaveName, boolean success)
      throws SlaveNotRegisterException {
    if (mUnregisterSlaves.contains(slaveName)) {
      throw new SlaveNotRegisterException(slaveName + " not register");
    }
    mReadySlaves.add(slaveName);
    if (!success) {
      mFailedSlaves.add(slaveName);
    }
  }

  /**
   * Register a slave.
   * 
   * @param slaveName the name of the slave
   * @param cleanupDir
   * @return if not null, it will cleanup this directory at the end of the test
   * @throws SlaveAlreadyRegisterException
   */
  public synchronized boolean slaveRegister(String slaveName, String cleanupDir)
      throws SlaveAlreadyRegisterException {
    if (mReadySlaves.contains(slaveName)) {
      throw new SlaveAlreadyRegisterException(slaveName + " already register");
    }
    mUnregisterSlaves.remove(slaveName);
    mReadySlaves.add(slaveName);
    if (cleanupDir != null) {
      mCleanupDirs.add(cleanupDir);
    }
    return true;
  }
}
