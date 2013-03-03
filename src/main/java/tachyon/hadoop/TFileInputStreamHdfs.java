package tachyon.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Config;
import tachyon.CommonUtils;
import tachyon.client.TFileInputStream;
import tachyon.client.TachyonClient;
import tachyon.client.TachyonFile;
import tachyon.thrift.NetAddress;
import tachyon.thrift.OutOfMemoryForPinFileException;

public class TFileInputStreamHdfs extends InputStream
implements Seekable, PositionedReadable {
  private static Logger LOG = LoggerFactory.getLogger(TFileInputStreamHdfs.class);

  private int mCurrentPosition;
  private TachyonClient mTachyonClient;
  private int mFileId;
  private Path mHdfsPath;
  private Configuration mHadoopConf;
  private int mHadoopBufferSize;

  private FSDataInputStream mHdfsInputStream = null;

  private TFileInputStream mTachyonPartitionInputStream = null;

  private int mBufferLimit = 0;
  private int mBufferPosition = 0;
  private byte mBuffer[] = new byte[Config.USER_BUFFER_PER_PARTITION_BYTES * 4];

  public TFileInputStreamHdfs(TachyonClient tachyonClient, int fileId, 
      Path hdfsPath, Configuration conf, int bufferSize) throws IOException {
    LOG.debug("PartitionInputStreamHdfs(" + tachyonClient + ", " + fileId + ", "
        + hdfsPath + ", " + conf + ", " + bufferSize + ")");
    mCurrentPosition = 0;
    mTachyonClient = tachyonClient;
    mFileId = fileId;
    mHdfsPath = hdfsPath;
    mHadoopConf = conf;
    mHadoopBufferSize = bufferSize;

    TachyonFile tachyonFile = mTachyonClient.getFile(mFileId);
    if (!tachyonFile.isReady()) {
      // Cache the partition
      long startTimeMs = System.currentTimeMillis();
      FileSystem fs = hdfsPath.getFileSystem(mHadoopConf);
      FSDataInputStream tHdfsInputStream = fs.open(mHdfsPath, mHadoopBufferSize);
      try {
        tachyonFile.open("w");
      } catch (IOException e) {
        LOG.error(e.getMessage());
        return;
      }
      int cnt = 0;

      int limit;
      while ((limit = tHdfsInputStream.read(mBuffer)) >= 0) {
        if (limit != 0) {
          try {
            tachyonFile.append(mBuffer, 0, limit);
          } catch (IOException e) {
            LOG.error(e.getMessage());
            return;
          } catch (OutOfMemoryForPinFileException e) {
            CommonUtils.runtimeException(e);
          }
        }
        cnt += limit;
      }

      tachyonFile.close();
      tachyonFile = mTachyonClient.getFile(mFileId);
      if (tachyonFile == null) {
        return;
      }
      LOG.info("Caching file " + mHdfsPath + " with size " + cnt + " bytes took " +
          (System.currentTimeMillis() - startTimeMs) + " ms. ");
    }
    try {
      tachyonFile.open("r");
    } catch (IOException e) {
      LOG.error(e.getMessage());
      return;
    }

    mTachyonPartitionInputStream = tachyonFile.getInputStream();
  }

  /**
   * Read upto the specified number of bytes, from a given position within a file, and return the
   * number of bytes read. This does not change the current offset of a file, and is thread-safe.
   */
  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    throw new IOException("Not supported");
    // TODO Auto-generated method stub
    //    return 0;
  }

  /**
   * Read number of bytes equalt to the length of the buffer, from a given position within a file.
   * This does not change the current offset of a file, and is thread-safe.
   */
  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    // TODO Auto-generated method stub
    throw new IOException("Not supported");
  }

  /**
   * Read the specified number of bytes, from a given position within a file. This does not
   * change the current offset of a file, and is thread-safe.
   */
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    // TODO Auto-generated method stub
    throw new IOException("Not supported");
  }

  /**
   * Return the current offset from the start of the file
   */
  @Override
  public long getPos() throws IOException {
    return mCurrentPosition;
  }

  /**
   * Seek to the given offset from the start of the file.
   * The next read() will be from that location.  Can't seek past the end of the file.
   */
  @Override
  public void seek(long pos) throws IOException {
    if (pos == mCurrentPosition) {
      return;
    }
    throw new IOException("Not supported to seek to " + pos + " . Current Position is " 
        + mCurrentPosition);
  }

  /**
   * Seeks a different copy of the data.  Returns true if found a new source, false otherwise.
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException("Not supported");
    // TODO Auto-generated method stub
    //    return false;
  }

  @Override
  public int read() throws IOException {
    if (mHdfsInputStream != null) {
      return readFromHdfsBuffer();
    }

    if (mTachyonPartitionInputStream != null) {
      int ret = 0;
      try {
        ret = mTachyonPartitionInputStream.read();
        mCurrentPosition ++;
        return ret;
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        mTachyonPartitionInputStream = null;
      }
    }

    FileSystem fs = mHdfsPath.getFileSystem(mHadoopConf);
    mHdfsInputStream = fs.open(mHdfsPath, mHadoopBufferSize);
    mHdfsInputStream.seek(mCurrentPosition);

    return readFromHdfsBuffer();
  }

  private int readFromHdfsBuffer() throws IOException {
    if (mBufferPosition < mBufferLimit) {
      return mBuffer[mBufferPosition ++];
    }
    while ((mBufferLimit = mHdfsInputStream.read(mBuffer)) == 0) {
      LOG.error("Read 0 bytes in readFromHdfsBuffer for " + mHdfsPath); 
    }
    if (mBufferLimit == -1) {
      return -1;
    }
    mBufferPosition = 0;
    return mBuffer[mBufferPosition ++];
  }
  
  @Override
  public void close() throws IOException {
    if (mTachyonPartitionInputStream != null) {
      mTachyonPartitionInputStream.close();
    }
    if (mHdfsInputStream != null) {
      mHdfsInputStream.close();
    }
  }
}