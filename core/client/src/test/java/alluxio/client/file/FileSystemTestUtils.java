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

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

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
   */
  public static void createByteFile(FileSystem fs, String fileName, int len,
      CreateFileOptions options) {
    createByteFile(fs, new AlluxioURI(fileName), options, len);
  }

  /**
   * Creates a simple file with {@code len} bytes.
   *
   * @param fs a {@link FileSystem} handler
   * @param fileName the name of the file to be created
   * @param writeType {@link WriteType} used to create the file
   * @param len file size
   */
  public static void createByteFile(FileSystem fs, String fileName,
      WriteType writeType, int len) {
    createByteFile(fs, new AlluxioURI(fileName), writeType, len);
  }

  /**
   * Creates a simple file with {@code len} bytes.
   *
   * @param fs a {@link FileSystem} handler
   * @param fileURI URI of the file
   * @param writeType {@link WriteType} used to create the file
   * @param len file size
   */
  public static void createByteFile(FileSystem fs, AlluxioURI fileURI,
      WriteType writeType, int len) {
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
   */
  public static void createByteFile(FileSystem fs, AlluxioURI fileURI, CreateFileOptions options,
      int len) {
    try (FileOutStream os = fs.createFile(fileURI, options)) {
      byte[] arr = new byte[len];
      for (int k = 0; k < len; k++) {
        arr[k] = (byte) k;
      }
      os.write(arr);
    } catch (IOException | AlluxioException e) {
      throw new RuntimeException(e);
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
   */
  public static void createByteFile(FileSystem fs, String fileName, WriteType writeType, int len,
      long blockCapacityByte) {
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
   */
  public static List<String> listFiles(FileSystem fs, String path) {
    try {
      List<URIStatus> statuses = fs.listStatus(new AlluxioURI(path));
      List<String> res = new ArrayList<>();
      for (URIStatus status : statuses) {
        res.add(status.getPath());
        if (status.isFolder()) {
          res.addAll(listFiles(fs, status.getPath()));
        }
      }
      return res;
    } catch (IOException | AlluxioException e) {
      throw new RuntimeException(e);
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
    if (op.getWriteType().getAlluxioStorageType().isStore()) {
      return OpenFileOptions.defaults().setReadType(ReadType.CACHE);
    }
    return OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
  }

  private FileSystemTestUtils() {} // prevent instantiation
}
