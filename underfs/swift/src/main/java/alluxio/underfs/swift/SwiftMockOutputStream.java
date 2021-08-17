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

package alluxio.underfs.swift;

import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.UUID;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for writing data to an in-memory simulated Swift client. The data will be written to a
 * file in a temporary directory and written out all at once.
 */
@NotThreadSafe
public class SwiftMockOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(SwiftMockOutputStream.class);

  private final File mFile;

  private final OutputStream mOutputStream;

  private final String mObjectName;

  private final Account mAccount;

  private final String mContainerName;

  /** Flag to indicate if this stream has been closed, to ensure close is only done once. */
  private boolean mClosed = false;

  /**
   * Creates a new instance of {@link SwiftMockOutputStream}.
   *
   * @param account simulated Swift account
   * @param containerName container name
   * @param objectName name of file or folder to write
   * @param tmpDirs a list of temporary directories
   */
  public SwiftMockOutputStream(Account account, String containerName, String objectName,
      List<String> tmpDirs)
      throws IOException {
    try {
      mAccount = account;
      mContainerName = containerName;
      mObjectName = objectName;
      mFile = new File(PathUtils.concatPath(CommonUtils.getTmpDir(tmpDirs), UUID.randomUUID()));
      mOutputStream  = new BufferedOutputStream(new FileOutputStream(mFile));
    } catch (Exception e) {
      LOG.error("Failed to construct SwiftMockOutputStream", e);
      throw new IOException(e);
    }
  }

  @Override
  public void write(int b) throws IOException {
    mOutputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mOutputStream.write(b, off, len);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mOutputStream.write(b);
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mOutputStream.close();

    try {
      Container container = mAccount.getContainer(mContainerName);
      StoredObject object = container.getObject(mObjectName);
      object.uploadObject(mFile);
    } catch (Exception e) {
      throw new IOException(e);
    }
    // Close successful
    mClosed = true;
  }

  @Override
  public void flush() throws IOException {
    mOutputStream.flush();
  }
}
