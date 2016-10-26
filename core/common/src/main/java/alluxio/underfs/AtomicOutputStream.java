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

/*
 * An {@link AtomicOutputStream} writes to a temporary file and renames on close
 */
public class AtomicOutputStream extends FilterOutputStream {

  private String mPermanentPath;
  private String mTemporaryPath;
  private UnderFileSystem mUfs;

  /*
   * Constructs a new {@link AtomicOutputStream}.
   *
   * @param permanentPath the final path
   * @param temporaryPath the path being written to
   * @param out the wrapped {@link OutputStream}
   * @param ufs the {@link UnderFileSystem} whose rename is invoked
   */
  public AtomicOutputStream(String permanentPath, String temporaryPath, OutputStream out,
                            UnderFileSystem ufs) {
    super(out);

    mPermanentPath = permanentPath;
    mTemporaryPath = temporaryPath;
    mUfs = ufs;
  }

  @Override
  public void close() throws IOException {
    out.close();

    if (!mUfs.rename(mTemporaryPath, mPermanentPath)) {
      mUfs.delete(mTemporaryPath, false);
      throw new IOException("Error renaming temporary path");
    }
  }
}

