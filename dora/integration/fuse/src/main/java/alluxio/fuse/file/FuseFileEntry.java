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

package alluxio.fuse.file;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Convenience class to encapsulate file stream
 * and its information (path, id) for reading or writing alluxio file.
 * @param <T> the concrete fuse file stream subclass
 */
@ThreadSafe
public final class FuseFileEntry<T extends FuseFileStream>
    implements Closeable {
  private final long mId;
  private final String mPath;

  private T mFileStream;

  private int mOpenOrCreateFlags;

  /**
   * Constructs a new {@link FuseFileEntry} for an Alluxio file.
   *
   * @param id the id of the file
   * @param path the path of the file
   * @param fileStream the in/out stream of the file
   * @param openOrCreateFlags open or create flags
   */
  public FuseFileEntry(long id, String path, T fileStream, int openOrCreateFlags) {
    Preconditions.checkArgument(id >= 0, "id should not be negative");
    Preconditions.checkArgument(path != null && !path.isEmpty(),
        "path should not be null or empty");
    mFileStream = Preconditions.checkNotNull(fileStream, "file stream cannot be null");
    mId = id;
    mPath = path;
    mOpenOrCreateFlags = openOrCreateFlags;
  }

  /**
   * @return the create or open flags when the file is opened
   */
  public int getOpenOrCreateFlags() {
    return mOpenOrCreateFlags;
  }

  /**
   * @param openOrCreateFlags the open or create flags
   */
  public void setOpenOrCreateFlags(int openOrCreateFlags) {
    mOpenOrCreateFlags = openOrCreateFlags;
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
   * Gets the fuse file stream for this entry.
   *
   * @return a fuse file stream
   */
  public T getFileStream() {
    return mFileStream;
  }

  /**
   * @param fileStream sets the file stream
   */
  public void setFileStream(T fileStream) {
    mFileStream = fileStream;
  }

  /**
   * Closes the underlying open streams.
   */
  @Override
  public void close() throws IOException {
    mFileStream.close();
  }
}
