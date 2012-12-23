package tachyon.hadoop;

import java.io.IOException;
import java.io.InputStream;

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
import tachyon.client.Partition;
import tachyon.client.PartitionInputStream;
import tachyon.client.Dataset;
import tachyon.client.TachyonClient;
import tachyon.thrift.OutOfMemoryForPinDatasetException;
import tachyon.thrift.PartitionInfo;

public class PartitionInputStreamHdfs extends InputStream
implements Seekable, PositionedReadable {
  private static Logger LOG = LoggerFactory.getLogger(PartitionInputStreamHdfs.class);

  private int mCurrentPosition;
  private TachyonClient mTachyonClient;
  private int mDatasetId;
  private int mPartitionId;
  private Path mHdfsPath;
  private Configuration mHadoopConf;
  private int mHadoopBufferSize;

  private FSDataInputStream mHdfsInputStream = null;

  private Dataset mDataset = null;
  private PartitionInputStream mTachyonPartitionInputStream = null;

  private int mBufferLimit = 0;
  private int mBufferPosition = 0;
  private byte mBuffer[] = new byte[Config.USER_BUFFER_PER_PARTITION_BYTES * 4];

  public PartitionInputStreamHdfs(TachyonClient tachyonClient, int datasetId, int partitionId, 
      Path hdfsPath, Configuration conf, int bufferSize) throws IOException {
    LOG.debug("PartitionInputStreamHdfs(" + tachyonClient + ", " + datasetId + ", " + partitionId + ", "
        + hdfsPath + ", " + conf + ", " + bufferSize + ")");
    mCurrentPosition = 0;
    mTachyonClient = tachyonClient;
    mDatasetId = datasetId;
    mPartitionId = partitionId;
    mHdfsPath = hdfsPath;
    mHadoopConf = conf;
    mHadoopBufferSize = bufferSize;

    mDataset = mTachyonClient.getDataset(mDatasetId);
    if (mDataset == null || !mDataset.needCache()) {
      return;
    }
    PartitionInfo pInfo = mDataset.getPartitionInfo(partitionId);
    if (pInfo == null) {
      return;
    }
    Partition TachyonPartition = null;
    if (pInfo.mLocations.size() == 0) {
      // Cache the partition
      long startTimeMs = System.currentTimeMillis();
      FileSystem fs = hdfsPath.getFileSystem(mHadoopConf);
      FSDataInputStream tHdfsInputStream = fs.open(mHdfsPath, mHadoopBufferSize);
      TachyonPartition = mDataset.getPartition(mPartitionId);
      try {
        TachyonPartition.open("w");
      } catch (IOException e) {
        LOG.error(e.getMessage());
        return;
      }
      int cnt = 0;

      int limit;
      while ((limit = tHdfsInputStream.read(mBuffer)) >= 0) {
        if (limit != 0) {
          try {
            TachyonPartition.append(mBuffer, 0, limit);
          } catch (IOException e) {
            LOG.error(e.getMessage());
            return;
          } catch (OutOfMemoryForPinDatasetException e) {
            CommonUtils.runtimeException(e);
          }
        }
        cnt += limit;
      }

      TachyonPartition.close();
      mDataset = mTachyonClient.getDataset(mDatasetId);
      if (mDataset == null) {
        return;
      }
      LOG.info("Caching file " + mHdfsPath + " with size " + cnt + " bytes took " +
          (System.currentTimeMillis() - startTimeMs) + " ms. ");
    }
    TachyonPartition = mDataset.getPartition(mPartitionId);
    try {
      TachyonPartition.open("r");
    } catch (IOException e) {
      LOG.error(e.getMessage());
      return;
    }
    mTachyonPartitionInputStream = TachyonPartition.getInputStream();
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
}