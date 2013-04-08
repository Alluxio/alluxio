package tachyon;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.log4j.Logger;

import tachyon.conf.CommonConf;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;

public class MasterLogReader {
  private static final Logger LOG = Logger.getLogger(CommonConf.LOGGER_TYPE);

  private final String LOG_FILE_NAME;

  private Kryo mKryo;
  private Input mInput;

  private Pair<LogType, Object> mCurrent = null;

  public MasterLogReader(String fileName) throws IOException {
    LOG_FILE_NAME = fileName;
    mKryo = KryoFactory.createLogKryo();
    try {
      mInput = new Input(UnderFileSystem.getUnderFileSystem(LOG_FILE_NAME).open(LOG_FILE_NAME));
    } catch (FileNotFoundException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public boolean hasNext() {
    if (mCurrent == null) {
      mCurrent = getNext();
    }

    return mCurrent != null;
  }

  private Pair<LogType, Object> getNext() {
    LogType first = null;
    Object second = null;

    try {
      first = (LogType) mKryo.readClassAndObject(mInput);
      switch (first) {
        case CheckpointInfo:
          second = mKryo.readClassAndObject(mInput);
          break;
        case InodeFile:
          second = mKryo.readClassAndObject(mInput);
          break;
        case InodeFolder:
          second = mKryo.readClassAndObject(mInput);
          break;
        case InodeRawTable:
          second = mKryo.readClassAndObject(mInput);
          break;
        default:
          LOG.warn("Corrupted log.");
          break;
      }
    } catch (KryoException e) {
      LOG.warn(e.getMessage());
      return null;
    }

    return new Pair<LogType, Object>(first, second);
  }

  public Pair<LogType, Object> getNextPair() {
    if (mCurrent == null) {
      getNext();
    }

    Pair<LogType, Object> ret = mCurrent;
    mCurrent = null;
    return ret;
  }
}