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

package alluxio.perf.benchmark.simpleread;

import java.io.IOException;
import java.util.List;

import alluxio.perf.PerfConstants;
import alluxio.perf.basic.PerfThread;
import alluxio.perf.basic.TaskConfiguration;
import alluxio.perf.benchmark.ListGenerator;
import alluxio.perf.benchmark.Operators;
import alluxio.perf.conf.PerfConf;
import alluxio.perf.fs.PerfFS;

public class SimpleReadThread extends PerfThread {
  private int mBufferSize;
  private PerfFS mFileSystem;
  private List<String> mReadFiles;
  private String mReadType;

  private boolean mSuccess;
  private double mThroughput; // in MB/s

  @Override
  public boolean cleanupThread(TaskConfiguration taskConf) {
    try {
      mFileSystem.close();
    } catch (IOException e) {
      LOG.warn("Error when close file system, task " + mTaskId + " - thread " + mId, e);
    }
    return true;
  }

  public boolean getSuccess() {
    return mSuccess;
  }

  public double getThroughput() {
    return mThroughput;
  }

  @Override
  public void run() {
    long timeMs = System.currentTimeMillis();
    long readBytes = 0;
    mSuccess = true;
    for (String fileName : mReadFiles) {
      try {
        readBytes += Operators.readSingleFile(mFileSystem, fileName, mBufferSize, mReadType);
      } catch (IOException e) {
        LOG.error("Failed to read file " + fileName, e);
        mSuccess = false;
      }
    }
    timeMs = System.currentTimeMillis() - timeMs;
    mThroughput = (readBytes / 1024.0 / 1024.0) / (timeMs / 1000.0);
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mBufferSize = taskConf.getIntProperty("buffer.size.bytes");
    mReadType = taskConf.getProperty("read.type");
    try {
      mFileSystem = PerfConstants.getFileSystem();
      String readDir = taskConf.getProperty("read.dir");
      List<String> candidates = mFileSystem.listFullPath(readDir);
      if (candidates == null || candidates.isEmpty()) {
        throw new IOException("No file to read");
      }
      boolean isRandom = "RANDOM".equals(taskConf.getProperty("read.mode"));
      int filesNum = taskConf.getIntProperty("files.per.thread");
      if (isRandom) {
        mReadFiles = ListGenerator.generateRandomReadFiles(filesNum, candidates);
      } else {
        int threadNum = taskConf.getIntProperty("threads.num");
        threadNum = threadNum==0?PerfConf.get().THREADS_NUM:threadNum;
        mReadFiles =
            ListGenerator.generateSequenceReadFiles(mId, threadNum, filesNum,
                candidates);
      }
    } catch (IOException e) {
      LOG.error("Failed to setup thread, task " + mTaskId + " - thread " + mId, e);
      return false;
    }
    mSuccess = false;
    mThroughput = 0;
    return true;
  }
}
