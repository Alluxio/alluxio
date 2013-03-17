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
import org.apache.tools.ant.util.LazyFileOutputStream;

public class TachyonFileAppender extends FileAppender { 
  private int mMaxBackupIndex = 1;
  private int mMaxFileSize = 1;
  private int mCurrentFileBackupIndex = -1;
  private int mDeletionPercentage = 10;
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

  public void setDeletionPercentage(int deletionPercentage) {
    if (deletionPercentage > 0 && deletionPercentage <= 100) {
      mDeletionPercentage = deletionPercentage;
    } else {
      throw new RuntimeException("Log4j configuration error, invalid deletionPercentage");
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

  @Override
  public synchronized void setFile(String fileName, boolean append, boolean bufferedIO, 
      int bufferSize) throws IOException  {
    // It does not make sense to have immediate flush and bufferedIO.
    if(bufferedIO) {
      setImmediateFlush(false);
    }

    reset();

    //Creation of the LazyFileOutputStream object (the responsible of the log writing operations)
    LazyFileOutputStream ostream = new LazyFileOutputStream(fileName, append);

    Writer fw = createWriter(ostream);
    if(bufferedIO) {
      fw = new BufferedWriter(fw, bufferSize);
    }
    setQWForFiles(fw);
    this.fileName = fileName;
    this.fileAppend = append;
    this.bufferedIO = bufferedIO;
    this.bufferSize = bufferSize;
    writeHeader();
  }

  @Override
  public synchronized void subAppend(LoggingEvent event) {
    File currentLog = new File(mCurrentFileName);
    if (currentLog.length() > mMaxFileBytes || 
        !CommonUtils.convertMsToSimpleDate(System.currentTimeMillis()).equals(mLastDate)) {
      activateOptions();
    }
    super.subAppend(event);
  }


  private String getNewLogFileName(String fileName) {
    if (!fileName.isEmpty()) {
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
        newFileName = baseName + address + "_" 
            + CommonUtils.convertMsToSimpleDate(System.currentTimeMillis()) + suffix;
      }
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

  private void rotateLogs(String fileName) {
    String suffix = "";
    if (fileName.indexOf(".") != -1) {
      suffix = fileName.substring(fileName.lastIndexOf("."), fileName.length());
    }
    String prefix = fileName.substring(0, fileName.length() - suffix.length());

    if (mCurrentFileBackupIndex == -1) {
      int lo = 0;
      int hi = mMaxBackupIndex;
      while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        if (mid == 0) {
          mCurrentFileBackupIndex = 1;
          break;
        }
        System.out.println("mid is " + mid);
        if (new File(prefix + "_" + mid + suffix).exists()) {
          if (new File(prefix + "_" + (mid+1) + suffix).exists()) {
            lo = mid;
          } else {
            mCurrentFileBackupIndex = mid + 1;
            break;
          }
        } else {
          if (new File(prefix + "_" + (mid-1) + suffix).exists()) {
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
      int deleteToIndex = (int) Math.ceil(mMaxBackupIndex*mDeletionPercentage/100.0);
      for (int i = 1; i < deleteToIndex; i ++) {
        new File(prefix + "_" + i + suffix).delete();
      }
      for (int i = deleteToIndex + 1; i <= mMaxBackupIndex; i ++) {
        new File(prefix + "_" + i + suffix).renameTo(
            new File(prefix + "_" + (i - deleteToIndex) + suffix));
      }
      mCurrentFileBackupIndex = mCurrentFileBackupIndex - deleteToIndex;
    }
    oldFile.renameTo(new File(prefix + "_" + mCurrentFileBackupIndex + suffix));
    mCurrentFileBackupIndex ++;
  }
}