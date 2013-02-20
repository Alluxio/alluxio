package tachyon;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import tachyon.thrift.LogEventType;

public class MasterLogWriter {
  private final String LOG_FILE_NAME;

  private ObjectOutputStream mOutputStream;

  public MasterLogWriter(String fileName) {
    LOG_FILE_NAME = fileName;
    try {
      mOutputStream = new ObjectOutputStream(new FileOutputStream(LOG_FILE_NAME));
    } catch (FileNotFoundException e) {
      CommonUtils.runtimeException(e);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void appendAndFlush(Inode inode) {
    System.out.println("Append and Flush " + inode);
    try {
      mOutputStream.writeObject(LogEventType.INode);
      mOutputStream.writeObject(inode);
      mOutputStream.flush();
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }
  
  public synchronized void appendAndFlush(CheckpointInfo checkpointInfo) {
    try {
      mOutputStream.writeObject(LogEventType.CheckpointInfo);
      mOutputStream.writeObject(checkpointInfo);
      mOutputStream.flush();
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void close() {
    try {
      mOutputStream.close();
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }
}