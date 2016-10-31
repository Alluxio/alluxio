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

import alluxio.Constants;
import alluxio.underfs.options.NonAtomicCreateOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An {@link NonAtomicFileOutputStream} writes to a temporary file and renames on close.
 */
@NotThreadSafe
public class NonAtomicFileOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

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
    LOG.info("AMDEBUG writing to temp path {}", mParams.getTemporaryPath());
    mLocalOutputStream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    LOG.info("AMDEBUG writing to temp path {}", mParams.getTemporaryPath());
    mLocalOutputStream.write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    LOG.info("AMDEBUG writing to temp path {}", mParams.getTemporaryPath());
    mLocalOutputStream.write(b, off, len);
  }

  @Override
  public void close() throws IOException {
    LOG.info("AMDEBUG closing non atomic output stream: {} to {}", mParams.getTemporaryPath(),
        mParams.getPermanentPath());
    if (mClosed) {
      return;
    }
    LOG.info("AMDEBUG actually closing non atomic output stream: {} to {}",
        mParams.getTemporaryPath(), mParams.getPermanentPath());
    mLocalOutputStream.close();
    mUfs.completeCreate(mParams);
    mClosed = true;
  }
}

