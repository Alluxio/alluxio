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

package alluxio.perf.benchmark.mixture;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import alluxio.perf.PerfConstants;
import alluxio.perf.basic.PerfThread;
import alluxio.perf.basic.TaskConfiguration;
import alluxio.perf.benchmark.ListGenerator;
import alluxio.perf.benchmark.Operators;
import alluxio.perf.fs.PerfFS;

public class MixtureThread extends PerfThread {
  private Random mRand;

  private int mBasicFilesNum;
  private int mBlockSize;
  private int mBufferSize;
  private long mFileLength;
  private PerfFS mFileSystem;
  private int mReadFilesNum;
  private String mReadType;
  private String mWorkDir;
  private int mWriteFilesNum;
  private String mWriteType;

  private double mBasicWriteThroughput; // in MB/s
  private double mReadThroughput; // in MB/s
  private boolean mSuccess;
  private double mWriteThroughput; // in MB/s

  @Override
  public boolean cleanupThread(TaskConfiguration taskConf) {
    try {
      mFileSystem.close();
    } catch (IOException e) {
      LOG.warn("Error when close file system, task " + mTaskId + " - thread " + mId, e);
    }
    return true;
  }

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
    mFileSystem.create(mWorkDir + "/sync/" + mId);
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
    String dataDir = mWorkDir + "/data";
    String tmpDir = mWorkDir + "/tmp";

    long tTimeMs = System.currentTimeMillis();
    for (int b = 0; b < mBasicFilesNum; b ++) {
      try {
        String fileName = mId + "-" + b;
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
    while ((mReadFilesNum + mWriteFilesNum) > 0) {
      tTimeMs = System.currentTimeMillis();
      if (mRand.nextInt(mReadFilesNum + mWriteFilesNum) < mReadFilesNum) { // read
        try {
          List<String> candidates = mFileSystem.listFullPath(dataDir);
          String readFilePath = ListGenerator.generateRandomReadFiles(1, candidates).get(0);
          readBytes += Operators.readSingleFile(mFileSystem, readFilePath, mBufferSize, mReadType);
        } catch (IOException e) {
          LOG.error("Failed to read file", e);
          mSuccess = false;
        }
        readTimeMs += System.currentTimeMillis() - tTimeMs;
        mReadFilesNum --;
      } else { // write
        try {
          String writeFileName = mId + "--" + index;
          Operators.writeSingleFile(mFileSystem, tmpDir + "/" + writeFileName, mFileLength,
              mBlockSize, mBufferSize, mWriteType);
          writeBytes += mFileLength;
          mFileSystem.rename(tmpDir + "/" + writeFileName, dataDir + "/" + writeFileName);
        } catch (IOException e) {
          LOG.error("Failed to write file", e);
          mSuccess = false;
        }
        writeTimeMs += System.currentTimeMillis() - tTimeMs;
        mWriteFilesNum --;
      }
      index ++;
    }

    mBasicWriteThroughput =
        (basicBytes == 0) ? 0 : (basicBytes / 1024.0 / 1024.0) / (basicTimeMs / 1000.0);
    mReadThroughput = (readBytes == 0) ? 0 : (readBytes / 1024.0 / 1024.0) / (readTimeMs / 1000.0);
    mWriteThroughput =
        (writeBytes == 0) ? 0 : (writeBytes / 1024.0 / 1024.0) / (writeTimeMs / 1000.0);
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mRand = new Random(System.currentTimeMillis() + mTaskId + mId);
    mBasicFilesNum = taskConf.getIntProperty("basic.files.per.thread");
    mBlockSize = taskConf.getIntProperty("block.size.bytes");
    mBufferSize = taskConf.getIntProperty("buffer.size.bytes");
    mFileLength = taskConf.getLongProperty("file.length.bytes");
    mReadFilesNum = taskConf.getIntProperty("read.files.per.thread");
    mReadType = taskConf.getProperty("read.type");
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
    mBasicWriteThroughput = 0;
    mReadThroughput = 0;
    mWriteThroughput = 0;
    return true;
  }

  private void syncBarrier() throws IOException {
    String syncDirPath = mWorkDir + "/sync";
    String syncFileName = "" + mId;
    mFileSystem.delete(syncDirPath + "/" + syncFileName, false);
    while (!mFileSystem.listFullPath(syncDirPath).isEmpty()) {
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        LOG.error("Error in Sync Barrier", e);
      }
    }
  }
}
