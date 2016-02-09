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

package alluxio.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;

/**
 * Utility class for testing the Alluxio file system.
 */
@ThreadSafe
public final class FileSystemTestUtils {
  /**
   * Creates a simple file with {@code len} bytes.
   *
   * @param fs a {@link FileSystem} handler
   * @param fileName the name of the file to be created
   * @param len file size in bytes
   * @param options options to create the file with
   * @throws IOException if {@code path} is invalid (e.g., illegal URI)
   */
  public static void createByteFile(FileSystem fs, String fileName, int len,
      CreateFileOptions options) throws IOException {
    createByteFile(fs, new AlluxioURI(fileName), options, len);
  }

  /**
   * Creates a simple file with {@code len} bytes.
   *
   * @param fs a {@link FileSystem} handler
   * @param fileName the name of the file to be created
   * @param writeType {@link WriteType} used to create the file
   * @param len file size
   * @throws IOException if {@code path} is invalid (e.g., illegal URI)
   */
  public static void createByteFile(FileSystem fs, String fileName,
      WriteType writeType, int len) throws IOException {
    createByteFile(fs, new AlluxioURI(fileName), writeType, len);
  }

  /**
   * Creates a simple file with {@code len} bytes.
   *
   * @param fs a {@link FileSystem} handler
   * @param fileURI URI of the file
   * @param writeType {@link WriteType} used to create the file
   * @param len file size
   * @throws IOException if {@code path} is invalid (e.g., illegal URI)
   */
  public static void createByteFile(FileSystem fs, AlluxioURI fileURI,
      WriteType writeType, int len) throws IOException {
    CreateFileOptions options = CreateFileOptions.defaults().setWriteType(writeType);
    createByteFile(fs, fileURI, options, len);
  }

  /**
   * Creates a simple file with {@code len} bytes.
   *
   * @param fs a {@link FileSystem} handler
   * @param fileURI URI of the file
   * @param options client options to create the file with
   * @param len file size
   * @throws IOException if {@code path} is invalid (e.g., illegal URI)
   */
  public static void createByteFile(FileSystem fs, AlluxioURI fileURI,
      CreateFileOptions options, int len) throws IOException {
    try {
      FileOutStream os = fs.createFile(fileURI, options);

      byte[] arr = new byte[len];
      for (int k = 0; k < len; k++) {
        arr[k] = (byte) k;
      }
      os.write(arr);
      os.close();
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Creates a simple file with {@code len} bytes.
   *
   * @param fs a {@link FileSystem} handler
   * @param fileName the name of the file to be created
   * @param writeType {@link WriteType} used to create the file
   * @param len file size
   * @param blockCapacityByte block size of the file
   * @throws IOException if {@code path} is invalid (e.g., illegal URI)
   */
  public static void createByteFile(FileSystem fs, String fileName,
      WriteType writeType, int len, long blockCapacityByte) throws IOException {
    CreateFileOptions options =
        CreateFileOptions.defaults().setWriteType(writeType).setBlockSizeBytes(blockCapacityByte);
    createByteFile(fs, new AlluxioURI(fileName), options, len);
  }

  /**
   * Returns a list of files at a given {@code path}.
   *
   * @param fs a {@link FileSystem} handler
   * @param path a path in alluxio file system
   * @return a list of strings representing the file names under the given path
   * @throws IOException if {@code path} does not exist or is invalid
   */
  public static List<String> listFiles(FileSystem fs, String path) throws IOException {
    try {
      List<URIStatus> statuses = fs.listStatus(new AlluxioURI(path));
      List<String> res = new ArrayList<String>();
      for (URIStatus status : statuses) {
        res.add(status.getPath());
        if (status.isFolder()) {
          res.addAll(listFiles(fs, status.getPath()));
        }
      }
      return res;
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Converts a {@link CreateFileOptions} object to an {@link OpenFileOptions} object with a
   * matching Alluxio storage type.
   *
   * @param op a {@link CreateFileOptions} object
   * @return an {@link OpenFileOptions} object with a matching Alluxio storage type
   */
  public static OpenFileOptions toOpenFileOptions(CreateFileOptions op) {
    if (op.getAlluxioStorageType().isStore()) {
      return OpenFileOptions.defaults().setReadType(ReadType.CACHE);
    }
    return OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
  }

  private FileSystemTestUtils() {} // prevent instantiation
}
