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

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Convenience class to encapsulate input/output streams of open alluxio files.
 *
 * An open file can be either write-only or read-only, never both. This means that one of getIn or
 * getOut will be null, while the other will be non-null. It is up to the user of this class
 * (currently, only {@link AlluxioFuseFileSystem}) to check that.
 *
 * This mechanism is preferred over more complex sub-classing to avoid useless casts or type checks
 * for every read/write call, which happen quite often.
 *
 * @param <T1> the concrete input stream subclass
 * @param <T2> the concrete output stream subclass
 */
@NotThreadSafe
public final class OpenFileEntry<T1 extends InputStream, T2 extends OutputStream>
    implements Closeable {
  private final long mId;
  private final T1 mIn;
  private final T2 mOut;

  // Path is likely to be changed when fuse rename() is called
  private String mPath;
  /** the next write offset.  */
  private long mOffset;

  /**
   * Constructs a new {@link OpenFileEntry} for an Alluxio file.
   *
   * @param id the id of the file
   * @param path the path of the file
   * @param in the input stream of the file
   * @param out the output stream of the file
   */
  public OpenFileEntry(long id, String path, T1 in, T2 out) {
    Preconditions.checkArgument(id != -1 && !path.isEmpty());
    Preconditions.checkArgument(in != null || out != null);
    mId = id;
    mIn = in;
    mOut = out;
    mPath = path;
    mOffset = -1;
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
   * Gets the opened input stream for this open file entry. The value returned can be {@code null}
   * if the file is not open for reading.
   *
   * @return an opened input stream for the open alluxio file, or null
   */
  @Nullable
  public T1 getIn() {
    return mIn;
  }

  /**
   * Gets the opened output stream for this open file entry. The value returned can be {@code null}
   * if the file is not open for writing.
   *
   * @return an opened input stream for the open alluxio file, or null
   */
  @Nullable
  public T2 getOut() {
    return mOut;
  }

  /**
   * @return the offset of the next write
   */
  public long getWriteOffset() {
    return mOffset;
  }

  /**
   * Sets the path of the file. The file path can be changed
   * if fuse rename() is called.
   *
   * @param path the new path of the file
   */
  public void setPath(String path) {
    mPath = path;
  }

  /**
   * Sets the offset of the next write.
   *
   * @param offset the new offset of the next write
   */
  public void setWriteOffset(long offset) {
    mOffset = offset;
  }

  /**
   * Closes the underlying open streams.
   */
  @Override
  public void close() throws IOException {
    if (mIn != null) {
      mIn.close();
    }

    if (mOut != null) {
      mOut.close();
    }
    mOffset = -1;
  }
}
