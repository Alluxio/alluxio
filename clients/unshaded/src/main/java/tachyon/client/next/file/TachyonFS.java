/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.next.file;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.client.next.ClientOptions;
import tachyon.master.MasterClient;
import tachyon.thrift.FileInfo;

/**
 * Tachyon File System client. This class is the entry point for all file level operations on
 * Tachyon files. An instance of this class can be obtained via {@link TachyonFS#get}. This class
 * is thread safe. The read/write interface provided by this client is similar to Java's
 * input/output streams.
 */
public class TachyonFS implements Closeable, TachyonFSCore {
  private static TachyonFS sCachedClient;

  public static synchronized TachyonFS get() {
    if (null == sCachedClient) {
      sCachedClient = new TachyonFS();
    }
    return sCachedClient;
  }

  private FSContext mContext;

  private TachyonFS() {
    mContext = FSContext.INSTANCE;
  }

  // TODO: Evaluate the necessity of this method
  public synchronized void close() {
    sCachedClient = null;
  }

  public void delete(TachyonFile file) throws IOException {
    MasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.user_delete(file.getFileId(), "", true);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  public void free(TachyonFile file) throws IOException {
    MasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.user_freepath(file.getFileId(), "", true);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  public FileInfo getInfo(TachyonFile file) {
    // TODO: Implement me
    return null;
  }

  public FileInStream getInStream(TachyonFile file, ClientOptions options) {
    // TODO: Implement me
    return null;
  }

  public FileOutStream getOutStream(TachyonURI path, TachyonURI ufsPath, ClientOptions options) {
    // TODO: Implement me
    return null;
  }

  public List<FileInfo> listStatus(TachyonFile file) {
    // TODO: Implement me
    return null;
  }

  public boolean mkdirs(TachyonFile file) {
    // TODO: Implement me
    return false;
  }

  public TachyonFile open(TachyonURI path) {
    // TODO: Implement me
    return null;
  }

  public boolean rename(TachyonFile file, TachyonURI dst) {
    // TODO: Implement me
    return false;
  }
}
