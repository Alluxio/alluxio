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
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.options.OutStreamOptions;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;

public final class TachyonFSTestUtils {
  /**
   * Creates a simple file with <code>len</code> bytes.
   *
   * @param tfs a TachyonFileSystem handler
   * @param fileName the name of the file to be created
   * @param options client options to create the file with
   * @param len file size in bytes
   * @return the TachyonFile representation of the created file
   * @throws IOException if <code>path</code> is invalid (e.g., illegal URI)
   */
  public static TachyonFile createByteFile(TachyonFileSystem tfs, String fileName,
      OutStreamOptions options, int len) throws IOException {
    return createByteFile(tfs, fileName, options.getTachyonStorageType(),
        options.getUnderStorageType(), len, options.getBlockSize());
  }

  /**
   * Creates a simple file with <code>len</code> bytes.
   *
   * @param tfs a TachyonFileSystem handler
   * @param fileName the name of the file to be created
   * @param tachyonStorageType TachyonStorageType used to create the file
   * @param underStorageType UnderStorageType used to create the file
   * @param len file size
   * @return the TachyonFile of the created file
   * @throws IOException if <code>path</code> is invalid (e.g., illegal URI)
   */
  public static TachyonFile createByteFile(TachyonFileSystem tfs, String fileName,
      TachyonStorageType tachyonStorageType, UnderStorageType underStorageType, int len)
      throws IOException {
    return createByteFile(tfs, new TachyonURI(fileName), tachyonStorageType, underStorageType, len);
  }

  /**
   * Creates a simple file with <code>len</code> bytes.
   *
   * @param tfs a TachyonFileSystem handler
   * @param fileURI URI of the file
   * @param tachyonStorageType TachyonStorageType used to create the file
   * @param underStorageType UnderStorageType used to create the file
   * @param len file size
   * @return the TachyonFile of the created file
   * @throws IOException if <code>path</code> is invalid (e.g., illegal URI)
   */
  public static TachyonFile createByteFile(TachyonFileSystem tfs, TachyonURI fileURI,
      TachyonStorageType tachyonStorageType, UnderStorageType underStorageType, int len)
      throws IOException {
    try {
      OutStreamOptions options =
          new OutStreamOptions.Builder(ClientContext.getConf())
              .setTachyonStorageType(tachyonStorageType).setUnderStorageType(underStorageType)
              .build();
      FileOutStream os = tfs.getOutStream(fileURI, options);

      byte[] arr = new byte[len];
      for (int k = 0; k < len; k ++) {
        arr[k] = (byte) k;
      }
      os.write(arr);
      os.close();
      return tfs.open(fileURI);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Creates a simple file with <code>len</code> bytes.
   *
   * @param tfs a TachyonFileSystem handler
   * @param fileName the name of the file to be created
   * @param tachyonStorageType TachyonStorageType used to create the file
   * @param underStorageType UnderStorageType used to create the file
   * @param len file size
   * @param blockCapacityByte block size of the file
   * @return the TachyonFile of the created file
   * @throws IOException if <code>path</code> is invalid (e.g., illegal URI)
   */
  public static TachyonFile createByteFile(TachyonFileSystem tfs, String fileName,
      TachyonStorageType tachyonStorageType, UnderStorageType underStorageType, int len,
      long blockCapacityByte) throws IOException {
    try {
      OutStreamOptions options =
          new OutStreamOptions.Builder(ClientContext.getConf())
              .setTachyonStorageType(tachyonStorageType).setUnderStorageType(underStorageType)
              .setBlockSize(blockCapacityByte).build();
      FileOutStream os = tfs.getOutStream(new TachyonURI(fileName), options);

      for (int k = 0; k < len; k ++) {
        os.write((byte) k);
      }
      os.close();
      return tfs.open(new TachyonURI(fileName));
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Returns a list of files at a given <code>path</code>.
   *
   * @param tfs a TachyonFileSystem handler
   * @param path a path in tachyon file system
   * @return a list of strings representing the file names under the given path
   * @throws IOException if <code>path</code> does not exist or is invalid
   */
  public static List<String> listFiles(TachyonFileSystem tfs, String path) throws IOException {
    try {
      List<FileInfo> infos = tfs.listStatus(tfs.open(new TachyonURI(path)));
      List<String> res = new ArrayList<String>();
      for (FileInfo info : infos) {
        res.add(info.getPath());

        if (info.isFolder) {
          res.addAll(listFiles(tfs, info.getPath()));
        }
      }

      return res;
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  private TachyonFSTestUtils() {} // prevent instantiation
}
