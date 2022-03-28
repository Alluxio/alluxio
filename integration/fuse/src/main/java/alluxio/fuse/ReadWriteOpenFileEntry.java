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

package alluxio.fuse;

import alluxio.client.file.SeekableAlluxioFileOutStream;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Convenience class to encapsulate seekable output stream
 * and its information (path, id) for open alluxio file for read/write.
 */
@NotThreadSafe
public final class ReadWriteOpenFileEntry
    implements Closeable {
  private final long mId;
  private final String mPath;
  private final SeekableAlluxioFileOutStream mOut;

  /**
   * Constructs a new {@link ReadWriteOpenFileEntry} for an Alluxio file.
   *
   * @param id the id of the file
   * @param path the path of the file
   * @param out the seekable output stream of the file
   */
  public ReadWriteOpenFileEntry(long id, String path, SeekableAlluxioFileOutStream out) {
    Preconditions.checkArgument(id != -1 && !path.isEmpty());
    Preconditions.checkArgument(out != null);
    mId = id;
    mOut = out;
    mPath = path;
  }

  /**
   * @return the id of the file
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the path of the file
   */
  public String getPath() {
    return mPath;
  }

  /**
   * Gets the opened output stream for this open file entry.
   *
   * @return an opened input stream for the open alluxio file
   */
  public SeekableAlluxioFileOutStream getOut() {
    return mOut;
  }

  /**
   * Closes the underlying open streams.
   */
  @Override
  public void close() throws IOException {
    if (mOut != null) {
      mOut.close();
    }
  }
}
