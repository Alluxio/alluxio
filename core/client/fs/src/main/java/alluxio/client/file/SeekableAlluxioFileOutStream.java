/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.Seekable;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.io.FileUtils;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Supports seek method on top of FileOutStream. This output stream will skip writing data to
 * Alluxio but directly creating or writing to an file in UFS.
 * Note that, to make this experimental feature work, one must ensure
 * 1. the write type must be THROUGH.
 * 2. the UFS address must be like a local address (e.g., an NAS mount point)
 */
@NotThreadSafe
public class SeekableAlluxioFileOutStream extends FileOutStream implements Seekable {
  private static final Logger LOG = LoggerFactory.getLogger(SeekableAlluxioFileOutStream.class);
  private static final ListStatusPOptions OPTIONS = ListStatusPOptions.newBuilder()
      .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
          .setSyncIntervalMs(0).build())
      .build();

  private final String mUfsPath;
  private final AlluxioURI mAlluxioPath;
  private final FileSystem mFileSystem;
  private final RandomAccessFile mLocalFile;
  // state of this output stream
  public long mPos;
  private boolean mCanceled;
  private boolean mClosed;

  /**
   * Creates a new file output stream with seek.
   *
   * @param path the Alluxio path
   * @param ufsPath the corresponding ufs Path
   * @param fs the file system
   * @return a new file output stream with seek
   */
  public static SeekableAlluxioFileOutStream create(AlluxioURI path,
      String ufsPath, FileSystem fs) throws IOException {
    if (FileUtils.exists(ufsPath)) {
      throw new FileAlreadyExistsException(String.format("File %s already exists", ufsPath));
    }
    Path parent = Paths.get(ufsPath).getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    RandomAccessFile localFile =  new RandomAccessFile(ufsPath, "rw");
    try {
      fs.loadMetadata(path, OPTIONS);
    } catch (Exception e) {
      throw new IOException("Failed to update " + path, e);
    }
    return new SeekableAlluxioFileOutStream(path, ufsPath, fs, localFile);
  }

  /**
   * Creates a file output stream with seek. This file must be existing.
   *
   * @param path the file path
   * @param ufsPath the corresponding ufs Path
   * @param fs the file system
   * @return a new file output stream with seek
   */
  public static SeekableAlluxioFileOutStream open(AlluxioURI path,
      String ufsPath, FileSystem fs) throws IOException {
    if (!FileUtils.exists(ufsPath)) {
      throw new IOException(String.format("Can not find file %s", ufsPath));
    }
    RandomAccessFile localFile =  new RandomAccessFile(ufsPath, "rw");
    return new SeekableAlluxioFileOutStream(path, ufsPath, fs, localFile);
  }

  private SeekableAlluxioFileOutStream(AlluxioURI path, String ufsPath, FileSystem fs,
      RandomAccessFile localFile) {
    mAlluxioPath = path;
    mFileSystem = fs;
    mUfsPath = ufsPath;
    mLocalFile =  localFile;
    mPos = 0;
    mCanceled = false;
    mClosed = false;
    mBytesWritten = 0;
  }

  @Override
  public void write(int b) throws IOException {
    mLocalFile.write(b);
    mPos++;
    // bytesWritten is perhaps a confusing name in this case,
    // as we are using it to indicate file len effectively.
    mBytesWritten = Math.max(mPos, mBytesWritten);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mLocalFile.write(b, off, len);
    mPos += len;
    // bytesWritten is perhaps a confusing name in this case,
    // as we are using it to indicate file len effectively.
    mBytesWritten = Math.max(mPos, mBytesWritten);
  }

  /**
   * @param buf bytes to write to the file
   * @param len number of bytes to write
   */
  public void write(ByteBuffer buf, long len) throws IOException {
    int size = (int) len;
    byte[] tmp = new byte[size];
    buf.get(tmp, 0, size);
    write(tmp, 0 , size);
  }

  /**
   * @param buf bytes to read from the file
   * @param len number of bytes to read
   * @return number of bytes read
   */
  public int read(ByteBuffer buf, long len) throws IOException {
    int size = (int) len;
    final byte[] tmp = new byte[size];
    int rd = 0;
    int nread = 0;
    while (rd >= 0 && nread < size) {
      rd = mLocalFile.read(tmp, nread, size - nread);
      if (rd >= 0) {
        nread += rd;
      }
    }
    buf.put(tmp, 0, nread);
    return nread;
  }

  @Override
  public void seek(long pos) throws IOException {
    mLocalFile.seek(pos);
    mPos = pos;
    mBytesWritten = Math.max(mPos, mBytesWritten);
  }

  @Override
  public void flush() throws IOException {
    // flush is noop for RandomAccessFile
  }

  @Override
  public long getPos() throws IOException {
    return mPos;
  }

  @Override
  public void cancel() throws IOException {
    mCanceled = true;
    close();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mLocalFile.close();
    try {
      mFileSystem.loadMetadata(mAlluxioPath, OPTIONS);
    } catch (Exception e) {
      throw new IOException("Failed to update " + mAlluxioPath, e);
    } finally {
      mClosed = true;
    }
  }

  /**
   * @param length length in bytes of the out stream
   */
  public void setLength(long length) throws IOException {
    seek(length);
    mLocalFile.setLength(length);
  }

  @ThreadSafe
  private static final class Metrics {
    // Note that only counter can be added here.
    // Both meter and timer need to be used inline
    // because new meter and timer will be created after {@link MetricsSystem.resetAllMetrics()}
    private static final Counter BYTES_WRITTEN_UFS =
        MetricsSystem.counter(MetricKey.CLIENT_BYTES_WRITTEN_UFS.getName());

    private Metrics() {} // prevent instantiation
  }
}
