package tachyon;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.FileAppender;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;

public class TachyonFileAppender extends FileAppender { 
  private int mMaxBackupIndex = 1;
  private int mMaxFileSize = 1;
  private long mMaxFileBytes = mMaxFileSize * 1024 * 1024;
  private String mCurrentFileName = "";
  private String mOriginalFileName = "";
  private String mLastDate = "";

  public void setMaxBackupIndex(int maxBackups) {
    mMaxBackupIndex = maxBackups;
  }

  public void setMaxFileSize(int maxFileSize) {
    mMaxFileSize = maxFileSize;
  }

  @Override
  public void subAppend(LoggingEvent event) {
    File currentLog = new File(mCurrentFileName);
    if (currentLog.length() > mMaxFileBytes || 
        !CommonUtils.convertMsToSimpleDate(System.currentTimeMillis()).equals(mLastDate)) {
      activateOptions();
    }
    synchronized (this) {
      super.subAppend(event);
    }
  }

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
        errorHandler.error("Error while activating log options", e,
            ErrorCode.FILE_OPEN_FAILURE);
      }
    }
  }

  private String getNewLogFileName(String fileName) {
    if (fileName != null) {
      String newFileName = "";
      int dotIndex = fileName.indexOf(".");
      if (dotIndex != -1 && fileName.indexOf("-") == -1) {
        String baseName = fileName.substring(0, dotIndex);
        String suffix = fileName.substring(dotIndex, fileName.length());
        String address = "";
        try {
          address = "@" + InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException uhe) {
          address = "@UnknownHost";
        }
        newFileName = baseName + address + "(" 
            + CommonUtils.convertMsToSimpleDate(System.currentTimeMillis()) + ")" + suffix;
      }
      File file = new File(newFileName);
      if (file.exists()) {
        rotateLogs(newFileName);
        file = new File(newFileName);
      }
      mLastDate = CommonUtils.convertMsToSimpleDate(System.currentTimeMillis());
      mCurrentFileName = newFileName;
      return newFileName;
    }
    return null;
  }

  private void rotateLogs(String fileName) {
    String suffix = "";
    if (fileName.indexOf(".") != -1) {
      suffix = fileName.substring(fileName.lastIndexOf("."), fileName.length());
    }
    fileName = fileName.substring(0, fileName.length() - suffix.length());
    File latestFile = new File(fileName + suffix);
    if (latestFile.length() > 0) {
      for (int i = mMaxBackupIndex; i > 0; i--) {  
        File oldFile = new File(fileName + "[" + i + "]" + suffix);
        if (oldFile.exists()) {
          if (i == mMaxBackupIndex) {
            oldFile.delete();
          } else {
            oldFile.renameTo(new File(fileName + "[" + (i + 1) + "]" + suffix));
          }
        }
      }
      latestFile.renameTo(new File(fileName + "[" + 1 + "]" + suffix));
    } else {
      latestFile.delete();
    }
  }
}