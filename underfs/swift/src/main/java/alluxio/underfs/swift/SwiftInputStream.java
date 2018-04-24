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

import alluxio.underfs.MultiRangeObjectInputStream;

import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.StoredObject;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for reading data from a Swift API based object store.
 * This class maintains the following invariant: mStream is set to null whenever a read operation
 * increments mPos to a chunk boundary.
 */
@NotThreadSafe
public class SwiftInputStream extends MultiRangeObjectInputStream {

  /** JOSS Swift account. */
  private final Account mAccount;
  /** Name of container the object resides in. */
  private final String mContainerName;
  /** The path of the object to read, without container prefix. */
  private final String mObjectPath;

  /**
   * Constructor for an input stream to an object in a Swift API based store.
   *
   * @param account JOSS account with authentication credentials
   * @param container the name of container where the object resides
   * @param object path of the object in the container
   */
  public SwiftInputStream(Account account, String container, String object) {
    this(account, container, object, 0L);
  }

  /**
   * Constructor for an input stream to an object in a Swift API based store.
   *
   * @param account JOSS account with authentication credentials
   * @param container the name of container where the object resides
   * @param object path of the object in the container
   * @param position the position to begin reading from
   */
  public SwiftInputStream(Account account, String container, String object, long position) {
    mAccount = account;
    mContainerName = container;
    mObjectPath = object;
    mPos = position;
  }

  @Override
  protected InputStream createStream(long startPos, long endPos) throws IOException {
    StoredObject storedObject = mAccount.getContainer(mContainerName).getObject(mObjectPath);
    DownloadInstructions downloadInstructions  = new DownloadInstructions();
    downloadInstructions.setRange(new MidPartLongRange(startPos, endPos - 1));
    return storedObject.downloadObjectAsInputStream(downloadInstructions);
  }
}
