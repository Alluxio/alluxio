package tachyon;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

import tachyon.thrift.LogEventType;

public class MasterLogReader {
  private final String LOG_FILE_NAME;

  private ObjectInputStream mInputStream;

  private Pair<LogEventType, Object> mCurrent = null;

  public MasterLogReader(String fileName) {
    LOG_FILE_NAME = fileName;
    try {
      mInputStream = new ObjectInputStream(new FileInputStream(LOG_FILE_NAME));
    } catch (FileNotFoundException e) {
      CommonUtils.runtimeException(e);
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public boolean hasNext() {
    if (mCurrent == null) {
      mCurrent = getNext();
    }

    return mCurrent != null;
  }
  
  private Pair<LogEventType, Object> getNext() {
    LogEventType first = null;
    Object second = null;
    
    try {
      first = (LogEventType) mInputStream.readObject();
      second = mInputStream.readObject();
    } catch (EOFException e) {
      return null;
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    } catch (ClassNotFoundException e) {
      CommonUtils.runtimeException(e);
    }
    
    return new Pair<LogEventType, Object>(first, second);
  }

  public Pair<LogEventType, Object> getNextPair() {
    if (mCurrent == null) {
      getNext();
    }

    Pair<LogEventType, Object> ret = mCurrent;
    mCurrent = null;
    return ret;
  }
}