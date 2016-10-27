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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An {@link AtomicFileOutputStream} writes to a temporary file and renames on close.
 */
@NotThreadSafe
public class AtomicFileOutputStream extends FilterOutputStream {

  private String mTemporaryPath;
  private String mPermanentPath;
  private boolean mClosed = false;
  private NonAtomicCreateUnderFileSystem mUfs;

  /**
   * Constructs a new {@link AtomicFileOutputStream}.
   *
   * @param permanentPath the final path of the file
   * @param temporaryPath the temporary path to write to
   * @param out the wrapped {@link OutputStream}
   * @param ufs the calling {@link NonAtomicCreateUnderFileSystem}
   */
  public AtomicFileOutputStream(String permanentPath, String temporaryPath, OutputStream out,
                                NonAtomicCreateUnderFileSystem ufs) {
    super(out);

    mPermanentPath = permanentPath;
    mTemporaryPath = temporaryPath;
    mUfs = ufs;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    out.close();
    mUfs.completeCreate(mTemporaryPath, mPermanentPath);
    mClosed = true;
  }
}

