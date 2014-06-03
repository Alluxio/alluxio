/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.lang.Math;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.FileAppender;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;
import tachyon.util.CommonUtils;

/**
 * Custom log4j appender which preserves old logs on system restart, rolls over logs based on both
 * size and day. Also implements batch deletion of logs when the maximum backup index is reached.
 */
public class Log4jFileAppender extends FileAppender {
  private int mMaxBackupIndex = 1;
  private int mMaxFileSizeBytes = Constants.MB;
  private int mCurrentFileBackupIndex = -1;
  private int mDeletionPercentage = 10;
  private String mCurrentFileName = "";
  private String mOriginalFileName = "";
  private String mLastDate = "";

  /**
   * Called when a new log attempt is made, either due to server restart or rollover. The filename
   * is modified to identify the logging node in getNewFileName.
   */
  @Override
  public void activateOptions() {
    if (fileName != null) {
      if (!fileName.equals(mCurrentFileName)) {
        mOriginalFileName = fileName;
      } else {
        fileName = mOriginalFileName;
      }
      try {
        fileName = getNewLogFileName(fileName);
        setFile(fileName, fileAppend, bufferedIO, bufferSize);
      } catch (Exception e) {
        errorHandler.error("Error while activating log options", e, ErrorCode.FILE_OPEN_FAILURE);
      }
    }
  }

  /**
   * Gets a log file name which includes the logger's host address and the date.
   * 
   * @param fileName
   *          The base filename
   * @return A new filename string
   */
  private String getNewLogFileName(String fileName) {
    if (!fileName.isEmpty()) {
      String newFileName = "";
      String address = "";
      try {
        address = "@" + InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException uhe) {
        address = "@UnknownHost";
      }
      newFileName =
          fileName + address + "_" + CommonUtils.convertMsToSimpleDate(System.currentTimeMillis());
      File file = new File(newFileName);
      if (file.exists()) {
        rotateLogs(newFileName);
      }
      mLastDate = CommonUtils.convertMsToSimpleDate(System.currentTimeMillis());
      mCurrentFileName = newFileName;
      return newFileName;
    } else {
      throw new RuntimeException("Log4j configuration error, null filepath");
    }
  }

  /**
   * Rotates logs. The previous current log is set to the next available index. If the index has
   * reached the maximum backup index, a percent of backup logs will be deleted, started from the
   * earliest first. Then all rolledover logs will be moved up.
   * 
   * @param fileName
   *          The fileName of the new current log.
   */
  private void rotateLogs(String fileName) {
    if (mCurrentFileBackupIndex == -1) {
      int lo = 0;
      int hi = mMaxBackupIndex;
      while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        if (mid == 0) {
          mCurrentFileBackupIndex = 1;
          break;
        }
        if (new File(fileName + "_" + mid).exists()) {
          if (new File(fileName + "_" + (mid + 1)).exists()) {
            lo = mid;
          } else {
            mCurrentFileBackupIndex = mid + 1;
            break;
          }
        } else {
          if (new File(fileName + "_" + (mid - 1)).exists()) {
            mCurrentFileBackupIndex = mid;
            break;
          } else {
            hi = mid;
          }
        }
      }
    }

    File oldFile = new File(fileName);
    if (mCurrentFileBackupIndex >= mMaxBackupIndex) {
      int deleteToIndex = (int) Math.ceil(mMaxBackupIndex * mDeletionPercentage / 100.0);
      for (int i = 1; i < deleteToIndex; i ++) {
        new File(fileName + "_" + i).delete();
      }
      for (int i = deleteToIndex + 1; i <= mMaxBackupIndex; i ++) {
        new File(fileName + "_" + i).renameTo(new File(fileName + "_" + (i - deleteToIndex)));
      }
      mCurrentFileBackupIndex = mCurrentFileBackupIndex - deleteToIndex;
    }
    oldFile.renameTo(new File(fileName + "_" + mCurrentFileBackupIndex));
    mCurrentFileBackupIndex ++;
  }

  public void setDeletionPercentage(int deletionPercentage) {
    if (deletionPercentage > 0 && deletionPercentage <= 100) {
      mDeletionPercentage = deletionPercentage;
    } else {
      throw new RuntimeException("Log4j configuration error, invalid deletionPercentage");
    }
  }

  /**
   * Creates a LazyFileOutputStream so logs are only created when a message is logged.
   * 
   * @param fileName
   * @param append
   * @param bufferedIO
   * @param bufferSize
   */
  @Override
  public synchronized void setFile(String fileName, boolean append, boolean bufferedIO,
      int bufferSize) throws IOException {
    // It does not make sense to have immediate flush and bufferedIO.
    if (bufferedIO) {
      setImmediateFlush(false);
    }

    reset();

    // Creation of the LazyFileOutputStream object (the responsible of the log writing operations)
    LazyFileOutputStream ostream = new LazyFileOutputStream(fileName, append);

    Writer fw = createWriter(ostream);
    if (bufferedIO) {
      fw = new BufferedWriter(fw, bufferSize);
    }
    setQWForFiles(fw);
    this.fileName = fileName;
    this.fileAppend = append;
    this.bufferedIO = bufferedIO;
    this.bufferSize = bufferSize;
    writeHeader();
  }

  public void setMaxBackupIndex(int maxBackups) {
    mMaxBackupIndex = maxBackups;
  }

  public void setMaxFileSize(int maxFileSizeMB) {
    mMaxFileSizeBytes = maxFileSizeMB * Constants.MB;
  }

  /**
   * Called whenever a new message is logged. Checks both the date and size to determine if rollover
   * is necessary.
   * 
   * @param event
   */
  @Override
  public synchronized void subAppend(LoggingEvent event) {
    File currentLog = new File(mCurrentFileName);
    if (currentLog.length() > mMaxFileSizeBytes
        || !CommonUtils.convertMsToSimpleDate(System.currentTimeMillis()).equals(mLastDate)) {
      activateOptions();
    }
    if (currentLog.exists()) {
      super.subAppend(event);
    } else {
      String parentName = currentLog.getParent();
      if (parentName != null) {
        File parent = new File(parentName);
        if (parent.exists()) {
          super.subAppend(event);
        } else {
          if (parent.mkdirs()) {
            super.subAppend(event);
          }
        }
      }
    }
  }
}
