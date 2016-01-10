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
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;

/**
 * Utility class for testing the Tachyon file system.
 */
public final class TachyonFSTestUtils {
  /**
   * Creates a simple file with {@code len} bytes.
   *
   * @param tfs a {@link TachyonFileSystem} handler
   * @param fileName the name of the file to be created
   * @param len file size in bytes
   * @param options options to create the file with
   * @return the {@link TachyonFile} representation of the created file
   * @throws IOException if {@code path} is invalid (e.g., illegal URI)
   */
  public static TachyonFile createByteFile(TachyonFileSystem tfs, String fileName, int len,
      OutStreamOptions options) throws IOException {
    return createByteFile(tfs, new TachyonURI(fileName), options, len);
  }

  /**
   * Creates a simple file with {@code len} bytes.
   *
   * @param tfs a {@link TachyonFileSystem} handler
   * @param fileName the name of the file to be created
   * @param tachyonStorageType {@link TachyonStorageType} used to create the file
   * @param underStorageType {@link UnderStorageType} used to create the file
   * @param len file size
   * @return the {@link TachyonFile} of the created file
   * @throws IOException if {@code path} is invalid (e.g., illegal URI)
   */
  public static TachyonFile createByteFile(TachyonFileSystem tfs, String fileName,
      TachyonStorageType tachyonStorageType, UnderStorageType underStorageType, int len)
      throws IOException {
    return createByteFile(tfs, new TachyonURI(fileName), tachyonStorageType, underStorageType, len);
  }

  /**
   * Creates a simple file with {@code len} bytes.
   *
   * @param tfs a {@link TachyonFileSystem} handler
   * @param fileURI URI of the file
   * @param tachyonStorageType {@link TachyonStorageType} used to create the file
   * @param underStorageType {@link UnderStorageType} used to create the file
   * @param len file size
   * @return the {@link TachyonFile} of the created file
   * @throws IOException if {@code path} is invalid (e.g., illegal URI)
   */
  public static TachyonFile createByteFile(TachyonFileSystem tfs, TachyonURI fileURI,
      TachyonStorageType tachyonStorageType, UnderStorageType underStorageType, int len)
      throws IOException {
    OutStreamOptions options = new OutStreamOptions.Builder(ClientContext.getConf())
        .setTachyonStorageType(tachyonStorageType).setUnderStorageType(underStorageType).build();
    return createByteFile(tfs, fileURI, options, len);
  }

  /**
   * Creates a simple file with {@code len} bytes.
   *
   * @param tfs a {@link TachyonFileSystem} handler
   * @param fileURI URI of the file
   * @param options client options to create the file with
   * @param len file size
   * @return the {@link TachyonFile} of the created file
   * @throws IOException if {@code path} is invalid (e.g., illegal URI)
   */
  public static TachyonFile createByteFile(TachyonFileSystem tfs, TachyonURI fileURI,
      OutStreamOptions options, int len) throws IOException {
    try {
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
   * Creates a simple file with {@code len} bytes.
   *
   * @param tfs a {@link TachyonFileSystem} handler
   * @param fileName the name of the file to be created
   * @param tachyonStorageType {@link TachyonStorageType} used to create the file
   * @param underStorageType {@link UnderStorageType} used to create the file
   * @param len file size
   * @param blockCapacityByte block size of the file
   * @return the {@link TachyonFile} of the created file
   * @throws IOException if {@code path} is invalid (e.g., illegal URI)
   */
  public static TachyonFile createByteFile(TachyonFileSystem tfs, String fileName,
      TachyonStorageType tachyonStorageType, UnderStorageType underStorageType, int len,
      long blockCapacityByte) throws IOException {
    OutStreamOptions options = new OutStreamOptions.Builder(ClientContext.getConf())
        .setTachyonStorageType(tachyonStorageType).setUnderStorageType(underStorageType)
        .setBlockSizeBytes(blockCapacityByte).build();
    return createByteFile(tfs, new TachyonURI(fileName), options, len);
  }

  /**
   * Returns a list of files at a given {@code path}.
   *
   * @param tfs a {@link TachyonFileSystem} handler
   * @param path a path in tachyon file system
   * @return a list of strings representing the file names under the given path
   * @throws IOException if {@code path} does not exist or is invalid
   */
  public static List<String> listFiles(TachyonFileSystem tfs, String path) throws IOException {
    try {
      TachyonFile file = tfs.open(new TachyonURI(path));
      if (file == null) {
        throw new IOException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
      }
      List<FileInfo> infos = tfs.listStatus(file);
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

  /**
   * Converts an {@link OutStreamOptions} object to an {@link InStreamOptions} object with a
   * matching Tachyon storage type.
   *
   * @param op an {@link OutStreamOptions} object
   * @return an {@link InStreamOptions} object with a matching Tachyon storage type
   */
  public static InStreamOptions toInStreamOptions(OutStreamOptions op) {
    return new InStreamOptions.Builder(ClientContext.getConf())
        .setTachyonStorageType(op.getTachyonStorageType()).build();
  }

  private TachyonFSTestUtils() {} // prevent instantiation
}
