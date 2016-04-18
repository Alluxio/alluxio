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

package alluxio.perf.benchmark.iterate;

import java.io.IOException;
import java.util.List;

import alluxio.perf.PerfConstants;
import alluxio.perf.basic.PerfThread;
import alluxio.perf.basic.TaskConfiguration;
import alluxio.perf.benchmark.ListGenerator;
import alluxio.perf.benchmark.Operators;
import alluxio.perf.fs.PerfFS;

public class IterateThread extends PerfThread {
  protected int mBlockSize;
  protected int mBufferSize;
  protected long mFileLength;
  protected PerfFS mFileSystem;
  protected int mIterations;
  protected int mReadFilesNum;
  protected String mReadType;
  protected boolean mShuffle;
  protected String mWorkDir;
  protected int mWriteFilesNum;
  protected String mWriteType;

  protected double mReadThroughput; // in MB/s
  protected boolean mSuccess;
  protected double mWriteThroughput; // in MB/s

  public double getReadThroughput() {
    return mReadThroughput;
  }

  public boolean getSuccess() {
    return mSuccess;
  }

  public double getWriteThroughput() {
    return mWriteThroughput;
  }

  protected void initSyncBarrier() throws IOException {
    String syncFileName = mTaskId + "-" + mId;
    for (int i = 0; i < mIterations; i ++) {
      mFileSystem.create(mWorkDir + "/sync/" + i + "/" + syncFileName);
    }
  }

  protected void syncBarrier(int iteration) throws IOException {
    String syncDirPath = mWorkDir + "/sync/" + iteration;
    String syncFileName = mTaskId + "-" + mId;
    mFileSystem.delete(syncDirPath + "/" + syncFileName, false);
    while (!mFileSystem.listFullPath(syncDirPath).isEmpty()) {
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        LOG.error("Error in Sync Barrier", e);
      }
    }
  }

  @Override
  public void run() {
    long readBytes = 0;
    long readTimeMs = 0;
    long writeBytes = 0;
    long writeTimeMs = 0;
    mSuccess = true;
    for (int i = 0; i < mIterations; i ++) {
      String dataDir;
      if (mShuffle) {
        dataDir = mWorkDir + "/data/" + i;
      } else {
        dataDir = mWorkDir + "/data/" + mTaskId + "/" + i;
      }

      long tTimeMs = System.currentTimeMillis();
      for (int w = 0; w < mWriteFilesNum; w ++) {
        try {
          String fileName = mTaskId + "-" + mId + "-" + w;
          Operators.writeSingleFile(mFileSystem, dataDir + "/" + fileName, mFileLength,
              mBlockSize, mBufferSize, mWriteType);
          writeBytes += mFileLength;
        } catch (IOException e) {
          LOG.error("Failed to write file", e);
          mSuccess = false;
        }
      }
      writeTimeMs += System.currentTimeMillis() - tTimeMs;

      try {
        syncBarrier(i);
      } catch (IOException e) {
        LOG.error("Error in Sync Barrier", e);
        mSuccess = false;
      }
      tTimeMs = System.currentTimeMillis();
      try {
        List<String> candidates = mFileSystem.listFullPath(dataDir);
        List<String> readList = ListGenerator.generateRandomReadFiles(mReadFilesNum, candidates);
        for (String fileName : readList) {
          readBytes += Operators.readSingleFile(mFileSystem, fileName, mBufferSize, mReadType);
        }
      } catch (Exception e) {
        LOG.error("Failed to read file", e);
        mSuccess = false;
      }
      readTimeMs += System.currentTimeMillis() - tTimeMs;
    }
    mReadThroughput =
        (readTimeMs == 0) ? 0 : (readBytes / 1024.0 / 1024.0) / (readTimeMs / 1000.0);
    mWriteThroughput =
        (writeTimeMs == 0) ? 0 : (writeBytes / 1024.0 / 1024.0) / (writeTimeMs / 1000.0);
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mBlockSize = taskConf.getIntProperty("block.size.bytes");
    mBufferSize = taskConf.getIntProperty("buffer.size.bytes");
    mFileLength = taskConf.getLongProperty("file.length.bytes");
    mIterations = taskConf.getIntProperty("iterations");
    mReadFilesNum = taskConf.getIntProperty("read.files.per.thread");
    mReadType = taskConf.getProperty("read.type");
    mShuffle = taskConf.getBooleanProperty("shuffle.mode");
    mWorkDir = taskConf.getProperty("work.dir");
    mWriteFilesNum = taskConf.getIntProperty("write.files.per.thread");
    mWriteType = taskConf.getProperty("write.type");
    try {
      mFileSystem = PerfConstants.getFileSystem();
      initSyncBarrier();
    } catch (IOException e) {
      LOG.error("Failed to setup thread, task " + mTaskId + " - thread " + mId, e);
      return false;
    }
    mSuccess = false;
    mReadThroughput = 0;
    mWriteThroughput = 0;
    return true;
  }

  @Override
  public boolean cleanupThread(TaskConfiguration taskConf) {
    try {
      mFileSystem.close();
    } catch (IOException e) {
      LOG.warn("Error when close file system, task " + mTaskId + " - thread " + mId, e);
    }
    return true;
  }

}
