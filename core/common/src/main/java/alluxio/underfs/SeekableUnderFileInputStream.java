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

package alluxio.underfs;

import alluxio.Seekable;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;

/**
 * A seekable under file input stream wrapper that encapsulates the under file input stream.
 * Subclasses of this abstract class should implement the
 * {@link SeekableUnderFileInputStream#seek(long)} and the reposition the wrapped input stream.
 *
 * The {@link #mResourceId} is used by Alluxio worker to cache for reuse.
 */
public abstract class SeekableUnderFileInputStream extends InputStream implements Seekable {

  protected final InputStream mInputStream;
  /** A unique resource id annotated for resource tracking. */
  private Long mResourceId;
  /** The file path of the input stream. */
  private String mFilePath;

  /**
   * Creates a new {@link SeekableUnderFileInputStream}.
   *
   * @param inputStream the input stream from the under storage
   */
  public SeekableUnderFileInputStream(InputStream inputStream) {
    Preconditions.checkNotNull(inputStream, "inputStream");
    mInputStream = inputStream;
  }

  /**
   * Sets the resource id.
   *
   * @param resourceId the resource id
   */
  public void setResourceId(long resourceId) {
    Preconditions.checkArgument(resourceId >= 0, "resource id should be positive");
    mResourceId = resourceId;
  }

  /**
   * @return the resource id
   */
  public long getResourceId() {
    return mResourceId;
  }

  /**
   * Sets the under file path.
   *
   * @param filePath the file path
   */
  public void setFilePath(String filePath) {
    mFilePath = filePath;
  }

  /**
   * @return the under file path
   */
  public String getFilePath() {
    return mFilePath;
  }

  @Override
  public void close() throws IOException {
    mInputStream.close();
  }

  @Override
  public int read() throws IOException {
    return mInputStream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return mInputStream.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return mInputStream.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    return mInputStream.skip(n);
  }

  @Override
  public int available() throws IOException {
    return mInputStream.available();
  }

  @Override
  public synchronized void mark(int readlimit) {
    mInputStream.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    mInputStream.reset();
  }

  @Override
  public boolean markSupported() {
    return mInputStream.markSupported();
  }

  @Override
  public abstract void seek(long position) throws IOException;
}
