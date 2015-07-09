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
import tachyon.thrift.ClientFileInfo;

public final class TachyonFSTestUtils {
  /**
   * Create a simple file with <code>len</code> bytes.
   *
   * @param tfs
   * @param fileName
   * @param op
   * @param len
   * @return created file id.
   * @throws IOException
   */
  public static int createByteFile(TachyonFS tfs, String fileName, WriteType op, int len)
      throws IOException {
    return createByteFile(tfs, new TachyonURI(fileName), op, len);
  }

  /**
   * Create a simple file with <code>len</code> bytes.
   *
   * @param tfs
   * @param fileURI
   * @param op
   * @param len
   * @return
   * @throws IOException
   */
  public static int createByteFile(TachyonFS tfs, TachyonURI fileURI, WriteType op, int len)
      throws IOException {
    int fileId = tfs.createFile(fileURI);
    TachyonFile file = tfs.getFile(fileId);
    OutStream os = file.getOutStream(op);

    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();

    return fileId;
  }

  /**
   * Create a simple file with <code>len</code> bytes.
   *
   * @param tfs
   * @param fileName
   * @param op
   * @param len
   * @param blockCapacityByte
   * @return created file id.
   * @throws IOException
   */
  public static int createByteFile(TachyonFS tfs, String fileName, WriteType op, int len,
      long blockCapacityByte) throws IOException {
    int fileId = tfs.createFile(new TachyonURI(fileName), blockCapacityByte);
    TachyonFile file = tfs.getFile(fileId);
    OutStream os = file.getOutStream(op);

    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();

    return fileId;
  }

  public static List<String> listFiles(TachyonFS tfs, String path) throws IOException {
    List<ClientFileInfo> infos = tfs.listStatus(new TachyonURI(path));
    List<String> res = new ArrayList<String>();
    for (ClientFileInfo info : infos) {
      res.add(info.getPath());

      if (info.isFolder) {
        res.addAll(listFiles(tfs, info.getPath()));
      }
    }

    return res;
  }

}
