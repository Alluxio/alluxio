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

package alluxio.underfs.hdfs;

import alluxio.underfs.UnderFileSystemOutputStream;
import alluxio.util.UnderFileSystemUtils;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Output stream implementation for {@link HdfsUnderFileSystem}. This class is just a wrapper on top
 * of an underlying {@link FSDataOutputStream}, except all calls to {@link #flush()} will be
 * converted to {@link FSDataOutputStream#sync()}. This is currently safe because all invocations of
 * flush intend the functionality to be sync.
 */
@NotThreadSafe
public class HdfsUnderFileOutputStream extends OutputStream implements UnderFileSystemOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsUnderFileOutputStream.class);
  /** Underlying output stream. */
  private final FSDataOutputStream mOut;
  private final FileSystem mFs;
  private final String mPath;
  private String mContentHash = null;

  /**
   * Basic constructor.
   *
   * @param fs the hdfs file system object
   * @param path the path being written
   * @param out underlying stream to wrap
   */
  public HdfsUnderFileOutputStream(FileSystem fs, String path, FSDataOutputStream out) {
    mFs = fs;
    mPath = path;
    mOut = out;
  }

  @Override
  public void close() throws IOException {
    mOut.close();
    FileStatus fs = mFs.getFileStatus(new Path(mPath));
    // get the content hash immediately after the file has completed writing
    // which will be used for generating the fingerprint of the file in Alluxio
    // ideally this value would be received as a result from the close call
    // so that we would be sure to have the hash relating to the file uploaded
    mContentHash = UnderFileSystemUtils.approximateContentHash(
        fs.getLen(), fs.getModificationTime());
  }

  @Override
  public void flush() throws IOException {
    // TODO(calvin): This functionality should be restricted to select output streams.
    //#ifdef HADOOP1
    mOut.sync();
    //#else
    // Note that, hsync() flushes out the data in client's user buffer all the way to the disk
    // device which may result in much slower performance than sync().
    mOut.hsync();
    //#endif
  }

  @Override
  public void write(int b) throws IOException {
    mOut.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mOut.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mOut.write(b, off, len);
  }

  @Override
  public Optional<String> getContentHash() {
    return Optional.ofNullable(mContentHash);
  }
}
