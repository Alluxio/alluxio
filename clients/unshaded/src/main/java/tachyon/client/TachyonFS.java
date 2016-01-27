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

package tachyon.client;

import tachyon.TachyonURI;
import tachyon.client.file.FileSystem;

import java.io.IOException;

/**
 * Represents a Tachyon File System, legacy API.
 */
public class TachyonFS {
  private final FileSystem mFileSystem;

  public static TachyonFS get() {
    return new TachyonFS();
  }

  public TachyonFS() {
    mFileSystem = FileSystem.Factory.get();
  }

  public void close() {
    // Do nothing
  }

  public long createFile(TachyonURI path) {
    // Do nothing
    return -1;
  }

  public TachyonFile getFile(TachyonURI path) {
    return new TachyonFile(path, mFileSystem);
  }

  public boolean exist(TachyonURI path) throws IOException {
    try {
      return mFileSystem.exists(path);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public boolean mkdir(TachyonURI path) throws IOException {
    try {
      mFileSystem.createDirectory(path);
      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
