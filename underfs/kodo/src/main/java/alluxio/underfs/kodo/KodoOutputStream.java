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

package alluxio.underfs.kodo;

import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for writing a file into Qiniu. The data will be persisted to a temporary directory on
 * the local disk and copied as a complete file when the {@link #close()} method is called.
 */
@NotThreadSafe
public class KodoOutputStream extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(KodoOutputStream.class);

  private final String mKey;

  private final File mFile;

  private final KodoClient mKodoClient;

  private OutputStream mLocalOutputStream;

  private MessageDigest mHash;

  /**
   * Flag to indicate this stream has been closed, to ensure close is only done once.
   */
  private AtomicBoolean mClosed = new AtomicBoolean(false);

  /**
   * Creates a name instance of {@link KodoOutputStream}.
   *
   * @param key the key of the file
   * @param kodoClient the client for Kodo
   * @param tmpDirs a list of temporary directories
   */
  public KodoOutputStream(String key, KodoClient kodoClient, List<String> tmpDirs)
      throws IOException {
    mKey = key;
    mKodoClient = kodoClient;
    mFile = new File(PathUtils.concatPath(CommonUtils.getTmpDir(tmpDirs), UUID.randomUUID()));

    try {
      mHash = MessageDigest.getInstance("MD5");
      mLocalOutputStream =
          new BufferedOutputStream(new DigestOutputStream(new FileOutputStream(mFile), mHash));
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Algorithm not available for MD5 hash.", e);
      mHash = null;
      mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
    }
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
  public void flush() throws IOException {
    mLocalOutputStream.flush();
  }

  /**
   * Closes this output stream. When an output stream is closed, the local temporary file is
   * uploaded to KODO Service. Once the file is uploaded, the temporary file is deleted.
   */

  @Override
  public void close() {
    if (mClosed.getAndSet(true)) {
      return;
    }
    try {
      mLocalOutputStream.close();
      mKodoClient.uploadFile(mKey, mFile);
    } catch (Exception e) {
      LOG.error("Failed to upload {}.", mKey, e);
    } finally {
      // Delete the temporary file on the local machine if the Kodo client completed the
      // upload or if the upload failed.
      if (!mFile.delete()) {
        LOG.error("Failed to delete temporary file @ {}", mFile.getPath());
      }
    }
  }
}
