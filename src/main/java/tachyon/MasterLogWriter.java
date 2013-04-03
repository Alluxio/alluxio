package tachyon;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.conf.CommonConf;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class MasterLogWriter {
  private static final Logger LOG = Logger.getLogger(CommonConf.get().LOGGER_TYPE);

  private final String LOG_FILE_NAME;

  private Kryo mKryo;
  private Output mOutput;
  private FileOutputStream mFileOutputStream;

  public MasterLogWriter(String fileName) {
    LOG_FILE_NAME = fileName;
    mKryo = KryoFactory.createLogKryo();
    try {
      mFileOutputStream = new FileOutputStream(LOG_FILE_NAME);
      mOutput = new Output(mFileOutputStream);
    } catch (FileNotFoundException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void appendAndFlush(Inode inode) {
    LOG.debug("Append and Flush " + inode);
    if (inode.isFile()) {
      mKryo.writeClassAndObject(mOutput, LogType.InodeFile);
      mKryo.writeClassAndObject(mOutput, (InodeFile) inode);
    } else if (!((InodeFolder) inode).isRawTable()) {
      mKryo.writeClassAndObject(mOutput, LogType.InodeFolder);
      mKryo.writeClassAndObject(mOutput, (InodeFolder) inode);
    } else {
      mKryo.writeClassAndObject(mOutput, LogType.InodeRawTable);
      mKryo.writeClassAndObject(mOutput, (InodeRawTable) inode);
    }
    flush();
  }

  public void appendAndFlush(List<Inode> inodeList) {
    LOG.debug("Append and Flush List<Inode> " + inodeList);
    for (int k = 0; k < inodeList.size(); k ++) {
      Inode inode = inodeList.get(k);
      if (inode.isFile()) {
        mKryo.writeClassAndObject(mOutput, LogType.InodeFile);
        mKryo.writeClassAndObject(mOutput, (InodeFile) inode);
      } else if (!((InodeFolder) inode).isRawTable()) {
        mKryo.writeClassAndObject(mOutput, LogType.InodeFolder);
        mKryo.writeClassAndObject(mOutput, (InodeFolder) inode);
      } else {
        mKryo.writeClassAndObject(mOutput, LogType.InodeRawTable);
        mKryo.writeClassAndObject(mOutput, (InodeRawTable) inode);
      }
    }
    flush();
  }

  public synchronized void appendAndFlush(CheckpointInfo checkpointInfo) {
    mKryo.writeClassAndObject(mOutput, LogType.CheckpointInfo);
    mKryo.writeClassAndObject(mOutput, checkpointInfo);
    flush();
  }

  public synchronized void flush() {
    mOutput.flush();
    try {
      mFileOutputStream.flush();
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void close() {
    mOutput.close();
    try {
      mFileOutputStream.close();
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }
}