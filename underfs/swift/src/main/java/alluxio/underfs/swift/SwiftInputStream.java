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

import org.javaswift.joss.headers.object.range.ExcludeStartRange;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.StoredObject;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;

/**
 * A stream for reading data from a Swift API based object store.
 */
@NotThreadSafe
public class SwiftInputStream extends InputStream {
  /** JOSS Swift account. */
  private final Account mAccount;
  /** Name of container the object resides in. */
  private final String mContainerName;
  /** The path of the object to read, without container prefix. */
  private final String mObjectPath;

  /** The backing input stream. */
  private InputStream mStream;
  /** The current position of the stream. */
  private int mPos;

  /**
   * Constructor for an input stream to an object in a Swift API based store.
   *
   * @param account JOSS account with authentication credentials
   * @param container the name of container where the object resides
   * @param object path of the object in the container
   */
  public SwiftInputStream(Account account, String container, String object) {
    mAccount = account;
    mContainerName = container;
    mObjectPath = object;
  }

  @Override
  public void close() throws IOException {
    closeStream();
  }

  @Override
  public int read() throws IOException {
    if (mStream == null) {
      openStream();
    }
    int value = mStream.read();
    if (value != -1) { // valid data read
      mPos++;
    }
    return value;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int offset, int length) throws IOException {
    if (length == 0) {
      return 0;
    }
    if (mStream == null) {
      openStream();
    }
    int read = mStream.read(b, offset, length);
    if (read != -1) {
      mPos += read;
    }
    return read;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    closeStream();
    mPos += n;
    openStream();
    return n;
  }

  /**
   * Opens a new stream at mPos if the wrapped stream mIn is null.
   */
  private void openStream() {
    if (mStream != null) { // stream is already open
      return;
    }
    StoredObject storedObject = mAccount.getContainer(mContainerName).getObject(mObjectPath);
    DownloadInstructions downloadInstructions  = new DownloadInstructions();
    downloadInstructions.setRange(new ExcludeStartRange(mPos));
    mStream = storedObject.downloadObjectAsInputStream(downloadInstructions);
  }

  /**
   * Closes the current stream.
   */
  private void closeStream() throws IOException {
    if (mStream == null) {
      return;
    }
    mStream.close();
    mStream = null;
  }
}
