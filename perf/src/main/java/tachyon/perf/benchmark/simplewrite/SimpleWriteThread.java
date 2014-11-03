package tachyon.perf.benchmark.simplewrite;

import java.io.IOException;
import java.util.List;

import tachyon.perf.basic.PerfThread;
import tachyon.perf.basic.TaskConfiguration;
import tachyon.perf.benchmark.ListGenerator;
import tachyon.perf.benchmark.Operators;
import tachyon.perf.fs.PerfFileSystem;

public class SimpleWriteThread extends PerfThread {
  private int mBlockSize;
  private int mBufferSize;
  private long mFileLength;
  private PerfFileSystem mFileSystem;
  private List<String> mWriteFiles;
  private String mWriteType;

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
    long writeBytes = 0;
    mSuccess = true;
    for (String fileName : mWriteFiles) {
      try {
        Operators.writeSingleFile(mFileSystem, fileName, mFileLength, mBlockSize, mBufferSize,
            mWriteType);
        writeBytes += mFileLength;
      } catch (IOException e) {
        LOG.error("Failed to write file " + fileName, e);
        mSuccess = false;
      }
    }
    timeMs = System.currentTimeMillis() - timeMs;
    mThroughput = (writeBytes / 1024.0 / 1024.0) / (timeMs / 1000.0);
  }

  @Override
  public boolean setupThread(TaskConfiguration taskConf) {
    mBufferSize = taskConf.getIntProperty("buffer.size.bytes");
    mBlockSize = taskConf.getIntProperty("block.size.bytes");
    mFileLength = taskConf.getLongProperty("file.length.bytes");
    mWriteType = taskConf.getProperty("write.type");
    try {
      mFileSystem = PerfFileSystem.get();
      String writeDir = taskConf.getProperty("write.dir");
      int filesNum = taskConf.getIntProperty("files.per.thread");
      mWriteFiles = ListGenerator.generateWriteFiles(mId, filesNum, writeDir);
    } catch (IOException e) {
      LOG.error("Failed to setup thread, task " + mTaskId + " - thread " + mId, e);
      return false;
    }
    mSuccess = false;
    mThroughput = 0;
    return true;
  }
}
