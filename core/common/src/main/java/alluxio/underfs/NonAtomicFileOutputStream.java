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

import alluxio.underfs.options.NonAtomicCreateOptions;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An {@link NonAtomicFileOutputStream} writes to a temporary file and renames on close.
 */
@NotThreadSafe
public class NonAtomicFileOutputStream extends OutputStream {
  private OutputStream mLocalOutputStream;
  private NonAtomicCreateUnderFileSystem mUfs;
  private NonAtomicCreateOptions mParams;
  private boolean mClosed = false;

  /**
   * Constructs a new {@link NonAtomicFileOutputStream}.
   *
   * @param out the wrapped {@link OutputStream}
   * @param ufs the calling {@link NonAtomicCreateUnderFileSystem}
   * @param params options to complete create
   */
  public NonAtomicFileOutputStream(OutputStream out, NonAtomicCreateUnderFileSystem ufs,
                                   NonAtomicCreateOptions params) {
    mLocalOutputStream = out;
    mParams = params;
    mUfs = ufs;
  }

  @Override
  public void write(int b) throws IOException {
    mLocalOutputStream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mLocalOutputStream.write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mLocalOutputStream.write(b, off, len);
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mLocalOutputStream.close();
    mUfs.completeCreate(mParams);
    mClosed = true;
  }
}

