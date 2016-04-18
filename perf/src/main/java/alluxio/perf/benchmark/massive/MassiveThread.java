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

package alluxio.perf.benchmark.massive;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import alluxio.perf.PerfConstants;
import alluxio.perf.basic.PerfThread;
import alluxio.perf.basic.TaskConfiguration;
import alluxio.perf.benchmark.ListGenerator;
import alluxio.perf.benchmark.Operators;
import alluxio.perf.fs.PerfFS;

public class MassiveThread extends PerfThread {
  private Random mRand;

  private int mBasicFilesNum;
  private int mBlockSize;
  private int mBufferSize;
  private long mFileLength;
  private PerfFS mFileSystem;
  private long mLimitTimeMs;
  private String mReadType;
  private boolean mShuffle;
  private String mWorkDir;
  private String mWriteType;

  private double mBasicWriteThroughput; // in MB/s
  private double mReadThroughput; // in MB/s
  private boolean mSuccess;
  private double mWriteThroughput; // in MB/s

  public double getBasicWriteThroughput() {
    return mBasicWriteThroughput;
  }

  public double getReadThroughput() {
    return mReadThroughput;
  }

  public boolean getSuccess() {
    return mSuccess;
  }

  public double getWriteThroughput() {
    return mWriteThroughput;
  }

  private void initSyncBarrier() throws IOException {
    mFileSystem.create(mWorkDir + "/sync/" + mTaskId + "-" + mId);
  }

  private void syncBarrier() throws IOException {
    String syncDirPath = mWorkDir + "/sync";
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
    long basicBytes = 0;
    long basicTimeMs = 0;
    long readBytes = 0;
    long readTimeMs = 0;
    long writeBytes = 0;
    long writeTimeMs = 0;
    mSuccess = true;
    String tmpDir = mWorkDir + "/tmp";
    String dataDir;
    if (mShuffle) {
      dataDir = mWorkDir + "/data";
    } else {
      dataDir = mWorkDir + "/data/" + mTaskId;
    }

    long tTimeMs = System.currentTimeMillis();
    for (int b = 0; b < mBasicFilesNum; b ++) {
      try {
        String fileName = mTaskId + "-" + mId + "-" + b;
        Operators.writeSingleFile(mFileSystem, dataDir + "/" + fileName, mFileLength,
            mBlockSize, mBufferSize, mWriteType);
        basicBytes += mFileLength;
      } catch (IOException e) {
        LOG.error("Failed to write basic file", e);
        mSuccess = false;
        break;
      }
    }
    tTimeMs = System.currentTimeMillis() - tTimeMs;
    basicTimeMs += tTimeMs;

    try {
      syncBarrier();
    } catch (IOException e) {
      LOG.error("Error in Sync Barrier", e);
      mSuccess = false;
    }
    int index = 0;
    long limitTimeMs = System.currentTimeMillis() + mLimitTimeMs;
    while ((tTimeMs = System.currentTimeMillis()) < limitTimeMs) {
      if (mRand.nextBoolean()) { // read
        try {
          List<String> candidates = mFileSystem.listFullPath(dataDir);
          String readFilePath = ListGenerator.generateRandomReadFiles(1, candidates).get(0);
          readBytes += Operators.readSingleFile(mFileSystem, readFilePath, mBufferSize, mReadType);
        } catch (IOException e) {
          LOG.error("Failed to read file", e);
          mSuccess = false;
        }
        readTimeMs += System.currentTimeMillis() - tTimeMs;
      } else { // write
        try {
          String writeFileName = mTaskId + "-" + mId + "--" + index;
          Operators.writeSingleFile(mFileSystem, tmpDir + "/" + writeFileName, mFileLength,
              mBlockSize, mBufferSize, mWriteType);
          writeBytes += mFileLength;
          mFileSystem.rename(tmpDir + "/" + writeFileName, dataDir + "/" + writeFileName);
        } catch (IOException e) {
          LOG.error("Failed to write file", e);
          mSuccess = false;
        }
        writeTimeMs += System.currentTimeMillis() - tTimeMs;
      }
      index ++;
    }

    mBasicWriteThroughput =
        (basicTimeMs == 0) ? 0 : (basicBytes / 1024.0 / 1024.0) / (basicTimeMs / 1000.0);
    mReadThroughput =
        (readTimeMs == 0) ? 0 : (readBytes / 1024.0 / 1024.0) / (readTimeMs / 1000.0);
    mWriteThroughput =
        (writeTimeMs == 0) ? 0 : (writeBytes / 1024.0 / 1024.0) / (writeTimeMs / 1000.0);
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mRand = new Random(System.currentTimeMillis() + mTaskId + mId);
    mBlockSize = taskConf.getIntProperty("block.size.bytes");
    mBufferSize = taskConf.getIntProperty("buffer.size.bytes");
    mFileLength = taskConf.getLongProperty("file.length.bytes");
    mLimitTimeMs = taskConf.getLongProperty("time.seconds") * 1000;
    mReadType = taskConf.getProperty("read.type");
    mShuffle = taskConf.getBooleanProperty("shuffle.mode");
    mWorkDir = taskConf.getProperty("work.dir");
    mWriteType = taskConf.getProperty("write.type");
    mBasicFilesNum = taskConf.getIntProperty("basic.files.per.thread");
    try {
      mFileSystem = PerfConstants.getFileSystem();
      initSyncBarrier();
    } catch (IOException e) {
      LOG.error("Failed to setup thread, task " + mTaskId + " - thread " + mId, e);
      return false;
    }
    mSuccess = false;
    mBasicWriteThroughput = 0;
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
