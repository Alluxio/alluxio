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

import java.util.List;

import tachyon.client.next.ClientOptions;
import tachyon.client.next.TachyonURI;
import tachyon.thrift.FileInfo;

public class TachyonFS {

  public static TachyonFS get() {
    // TODO: Implement me
    return null;
  }

  public void deleteFile(TachyonFile file) {
    // TODO: Implement me
  }

  public void freeFile(TachyonFile file) {
    // TODO: Implement me
  }

  public TachyonFile getFile(TachyonURI path) {
    // TODO: Implement me
    return null;
  }

  public FileInfo getFileInfo(TachyonFile file) {
    // TODO: Implement me
    return null;
  }

  public FileInStream getFileInStream(TachyonFile file, ClientOptions options) {
    // TODO: Implement me
    return null;
  }

  public FileOutStream getFileOutStream(TachyonFile file, ClientOptions options) {
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

  public boolean promote(TachyonFile file) {
    // TODO: Implement me
    return false;
  }

  public boolean renameFile(TachyonFile file, TachyonURI dst) {
    // TODO: Implement me
    return false;
  }
}
