package tachyon;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

/**
 * <code>MasterLogWriter</code> writes log into master's write-ahead-log or checkpoint data files.
 */
public class MasterLogWriter {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final String LOG_FILE_NAME;

  private Kryo mKryo;
  private Output mOutput;
  private OutputStream mOutputStream;

  public MasterLogWriter(String fileName) throws IOException {
    LOG_FILE_NAME = fileName;
    mKryo = KryoFactory.createLogKryo();
    try {
      mOutputStream = UnderFileSystem.get(LOG_FILE_NAME).create(LOG_FILE_NAME, Integer.MAX_VALUE);
      mOutput = new Output(mOutputStream);
    } catch (FileNotFoundException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void append(Inode inode, boolean flush) {
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
    if (flush) {
      flush();
    }
  }

  public void append(List<Inode> inodeList, boolean flush) {
    LOG.debug("Append and Flush List<Inode> " + inodeList);
    for (int k = 0; k < inodeList.size(); k ++) {
      append(inodeList.get(k), false);
    }
    if (flush) {
      flush();
    }
  }

  public synchronized void appendAndFlush(CheckpointInfo checkpointInfo) {
    mKryo.writeClassAndObject(mOutput, LogType.CheckpointInfo);
    mKryo.writeClassAndObject(mOutput, checkpointInfo);
    flush();
  }

  public synchronized void flush() {
    mOutput.flush();
    try {
      mOutputStream.flush();
      if (mOutputStream instanceof FSDataOutputStream) {
        ((FSDataOutputStream) mOutputStream).sync();
      }
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }

  public synchronized void close() {
    mOutput.close();
    try {
      mOutputStream.close();
    } catch (IOException e) {
      CommonUtils.runtimeException(e);
    }
  }
}