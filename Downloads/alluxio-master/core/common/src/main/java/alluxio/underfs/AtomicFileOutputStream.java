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

import alluxio.exception.ExceptionMessage;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link AtomicFileOutputStream} writes to a temporary file and renames on close. This ensures
 * that writing to the stream is atomic, i.e., all writes become readable only after a close.
 */
@NotThreadSafe
public class AtomicFileOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(AtomicFileOutputStream.class);

  private AtomicFileOutputStreamCallback mUfs;
  private CreateOptions mOptions;
  private String mPermanentPath;
  private String mTemporaryPath;
  private OutputStream mTemporaryOutputStream;
  private boolean mClosed = false;

  /**
   * Constructs a new {@link AtomicFileOutputStream}.
   *
   * @param path path being written to
   * @param ufs the calling {@link UnderFileSystem}
   * @param options create options for destination file
   */
  public AtomicFileOutputStream(String path, AtomicFileOutputStreamCallback ufs,
      CreateOptions options) throws IOException {
    mOptions = options;
    mPermanentPath = path;
    mTemporaryPath = PathUtils.temporaryFileName(IdUtils.getRandomNonNegativeLong(), path);
    mTemporaryOutputStream = ufs.createDirect(mTemporaryPath, options);
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

    if (!mUfs.renameFile(mTemporaryPath, mPermanentPath)) {
      if (!mUfs.deleteFile(mTemporaryPath)) {
        LOG.error("Failed to delete temporary file {}", mTemporaryPath);
      }
      throw new IOException(
          ExceptionMessage.FAILED_UFS_RENAME.getMessage(mTemporaryPath, mPermanentPath));
    }

    // Preserve owner and group in case delegation was used to create the path
    if (mOptions.getOwner() != null || mOptions.getGroup() != null) {
      try {
        mUfs.setOwner(mPermanentPath, mOptions.getOwner(), mOptions.getGroup());
      } catch (Exception e) {
        LOG.warn("Failed to update the ufs ownership, default values will be used. " + e);
      }
    }
    // TODO(chaomin): consider setMode of the ufs file.
    mClosed = true;
  }
}

