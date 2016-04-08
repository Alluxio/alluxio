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

package alluxio.perf.benchmark.metadata;

import java.io.IOException;

import alluxio.perf.PerfConstants;
import alluxio.perf.basic.PerfThread;
import alluxio.perf.basic.TaskConfiguration;
import alluxio.perf.benchmark.Operators;
import alluxio.perf.fs.PerfFS;

public class MetadataThread extends PerfThread {
  private PerfFS[] mClients;
  private int mClientsNum;
  private int mOpTimeMs;
  private String mWorkDir;

  private double mRate; // in ops/sec
  private boolean mSuccess;

  @Override
  public boolean cleanupThread(TaskConfiguration taskConf) {
    for (int i = 0; i < mClientsNum; i ++) {
      try {
        mClients[i].close();
      } catch (IOException e) {
        LOG.warn("Error when close file system, task " + mTaskId + " - thread " + mId, e);
      }
    }
    return true;
  }

  public double getRate() {
    return mRate;
  }

  public boolean getSuccess() {
    return mSuccess;
  }

  @Override
  public void run() {
    int currentOps = 0;
    int nextClient = 0;
    mSuccess = true;
    long timeMs = System.currentTimeMillis();
    while ((System.currentTimeMillis() - timeMs) < mOpTimeMs) {
      try {
        currentOps += Operators.metadataSample(mClients[nextClient], mWorkDir + "/" + mId);
      } catch (IOException e) {
        LOG.error("Failed to do metadata operation", e);
        mSuccess = false;
      }
      nextClient = (nextClient + 1) % mClientsNum;
    }
    timeMs = System.currentTimeMillis() - timeMs;
    mRate = (currentOps / 1.0) / (timeMs / 1000.0);
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mClientsNum = taskConf.getIntProperty("clients.per.thread");
    mOpTimeMs = taskConf.getIntProperty("op.second.per.thread") * 1000;
    mWorkDir = taskConf.getProperty("work.dir");
    try {
      mClients = new PerfFS[mClientsNum];
      for (int i = 0; i < mClientsNum; i ++) {
        mClients[i] = PerfConstants.getFileSystem();
      }
    } catch (IOException e) {
      LOG.error("Failed to setup thread, task " + mTaskId + " - thread " + mId, e);
      return false;
    }
    mRate = 0;
    mSuccess = false;
    return true;
  }
}
