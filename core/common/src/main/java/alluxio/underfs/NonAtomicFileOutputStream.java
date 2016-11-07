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

import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.NonAtomicCreateOptions;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link NonAtomicFileOutputStream} writes to a temporary file and renames on close.
 */
@NotThreadSafe
public class NonAtomicFileOutputStream extends OutputStream {
  private OutputStream mTemporaryOutputStream;
  private NonAtomicUnderFileSystem mUfs;
  private NonAtomicCreateOptions mOptions;
  private boolean mClosed = false;

  /**
   * A {@link UnderFileSystem} which want to use a {@link NonAtomicFileOutputStream}.
   */
  public interface NonAtomicUnderFileSystem {
    /**
     * Create an output stream to a temporary path.
     *
     * @param path temporary path
     * @param options the options for create
     * @return a non atomic output stream
     * @throws IOException when create fails
     */
    OutputStream createTemporary(String path, CreateOptions options) throws IOException;

    /**
     * Complete the create operation by renaming temporary path to permanent.
     *
     * @param options information to complete create
     * @throws IOException when rename fails
     */
    void completeCreate(NonAtomicCreateOptions options) throws IOException;
  }

  /**
   * Constructs a new {@link NonAtomicFileOutputStream}.
   *
   * @param path path being written to
   * @param options create options for destination file
   * @param ufs the calling {@link UnderFileSystem}
   * @throws IOException when a non Alluxio error occurs
   */
  public NonAtomicFileOutputStream(String path, CreateOptions options,
      NonAtomicUnderFileSystem ufs) throws IOException {
    String temporaryPath = PathUtils.temporaryFileName(IdUtils.getRandomNonNegativeLong(), path);
    mOptions = new NonAtomicCreateOptions(temporaryPath, path, options);
    mTemporaryOutputStream = ufs.createTemporary(temporaryPath, options);
    mUfs = ufs;
  }

  @Override
  public void write(int b) throws IOException {
    mTemporaryOutputStream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mTemporaryOutputStream.write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mTemporaryOutputStream.write(b, off, len);
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mTemporaryOutputStream.close();
    mUfs.completeCreate(mOptions);
    mClosed = true;
  }
}

