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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.client.next.ClientOptions;
import tachyon.thrift.FileInfo;

public final class TachyonFSTestUtils {
  /**
   * Create a simple file with <code>len</code> bytes.
   *
   * @param tfs a TachyonFS handler
   * @param fileURI URI of the file
   * @param option The option to use when getting out stream
   * @param len file size
   * @return Id created file id
   * @throws java.io.IOException if <code>path</code> is invalid (e.g., illegal URI)
   */
  public static long createByteFile(TachyonFS tfs, TachyonURI fileURI, ClientOptions option,
      int len) throws IOException {
    FileOutStream os = tfs.getOutStream(fileURI, option);
    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();
    return tfs.open(fileURI).getFileId();
  }

  /**
   * List files at a given <code>path</code>.
   *
   * @param tfs a TachyonFS handler
   * @param path a path in tachyon file system
   * @return a list of stings representing the file names under the given path
   * @throws java.io.IOException if <code>path</code> does not exist or is invalid
   */
  public static List<String> listFiles(TachyonFS tfs, TachyonURI path) throws IOException {
    List<FileInfo> infos = tfs.listStatus(tfs.open(path));
    List<String> res = new ArrayList<String>();
    for (FileInfo info : infos) {
      res.add(info.getPath());
      if (info.isFolder) {
        res.addAll(listFiles(tfs, new TachyonURI(info.getPath())));
      }
    }
    return res;
  }
}
