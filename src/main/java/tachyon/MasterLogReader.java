package tachyon;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

import tachyon.thrift.DatasetInfo;

public class MasterLogReader {
  private final String LOG_FILE_NAME;

  private ObjectInputStream mInputStream;

  private DatasetInfo mCurrent = null;

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
      mCurrent = readNextDatasetInfo();
    }

    return mCurrent != null;
  }

  private DatasetInfo readNextDatasetInfo() {
    DatasetInfo ret = null;

    try {
      ret = (DatasetInfo) mInputStream.readObject();
    } catch (EOFException e) {
      return null;
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    } catch (ClassNotFoundException e) {
      CommonUtils.runtimeException(e);
    }

    return ret;
  }

  public DatasetInfo getNextDatasetInfo() {
    if (mCurrent == null) {
      readNextDatasetInfo();
    }

    DatasetInfo ret = mCurrent;
    mCurrent = null;
    return ret;
  }
}