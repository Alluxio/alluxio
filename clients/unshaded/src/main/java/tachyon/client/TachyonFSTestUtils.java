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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.client.next.file.FileOutStream;
import tachyon.thrift.FileInfo;

public final class TachyonFSTestUtils {
  /**
   * Creates a simple file with <code>len</code> bytes.
   *
   * @param tfs a TachyonFS handler
   * @param fileName the name of the file to be created
   * @param op WriteType used to create the file
   * @param len file size
   * @return Id of the created file
   * @throws IOException if <code>path</code> is invalid (e.g., illegal URI)
   */
  public static long createByteFile(TachyonFS tfs, String fileName, WriteType op, int len)
      throws IOException {
    return createByteFile(tfs, new TachyonURI(fileName), op, len);
  }

  /**
   * Creates a simple file with <code>len</code> bytes.
   *
   * @param tfs a TachyonFS handler
   * @param fileURI URI of the file
   * @param op WriteType used to create the file
   * @param len file size
   * @return Id created file id
   * @throws IOException if <code>path</code> is invalid (e.g., illegal URI)
   */
  public static long createByteFile(TachyonFS tfs, TachyonURI fileURI, WriteType op, int len)
      throws IOException {
    long fileId = tfs.createFile(fileURI);
    TachyonFile file = tfs.getFile(fileId);
    FileOutStream os = file.getOutStream(op);

    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();

    return fileId;
  }

  /**
   * Creates a simple file with <code>len</code> bytes.
   *
   * @param tfs a TachyonFS handler
   * @param fileName the name of the file to be created
   * @param op WriteType used to create the file
   * @param len file size
   * @param blockCapacityByte block size of the file
   * @return Id of the created file
   * @throws IOException if <code>path</code> is invalid (e.g., illegal URI)
   */
  public static long createByteFile(TachyonFS tfs, String fileName, WriteType op, int len,
      long blockCapacityByte) throws IOException {
    long fileId = tfs.createFile(new TachyonURI(fileName), blockCapacityByte);
    TachyonFile file = tfs.getFile(fileId);
    FileOutStream os = file.getOutStream(op);

    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();

    return fileId;
  }

  /**
   * Lists files at a given <code>path</code>.
   *
   * @param tfs a TachyonFS handler
   * @param path a path in tachyon file system
   * @return a list of stings representing the file names under the given path
   * @throws IOException if <code>path</code> does not exist or is invalid
   */
  public static List<String> listFiles(TachyonFS tfs, String path) throws IOException {
    List<FileInfo> infos = tfs.listStatus(new TachyonURI(path));
    List<String> res = new ArrayList<String>();
    for (FileInfo info : infos) {
      res.add(info.getPath());

      if (info.isFolder) {
        res.addAll(listFiles(tfs, info.getPath()));
      }
    }

    return res;
  }

}
