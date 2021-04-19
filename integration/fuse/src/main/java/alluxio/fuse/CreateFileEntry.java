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
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Convenience class to encapsulate output stream
 * and its information (path, id) for create alluxio file.
 * @param <T> the concrete output stream subclass
 */
@NotThreadSafe
public final class CreateFileEntry<T extends OutputStream>
    implements Closeable {
  private final long mId;
  private final T mOut;
  // Path is likely to be changed when fuse rename() is called
  private String mPath;

  /**
   * Constructs a new {@link CreateFileEntry} for an Alluxio file.
   *
   * @param id the id of the file
   * @param path the path of the file
   * @param out the output stream of the file
   */
  public CreateFileEntry(long id, String path, T out) {
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
   * Gets the opened output stream for this open file entry. The value returned can be {@code null}
   * if the file is not open for writing.
   *
   * @return an opened input stream for the open alluxio file, or null
   */
  public T getOut() {
    return mOut;
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
   * Closes the underlying open streams.
   */
  @Override
  public void close() throws IOException {
    if (mOut != null) {
      mOut.close();
    }
  }
}
