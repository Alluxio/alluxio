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

package alluxio.master.backcompat;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.grpc.CreateFilePOptions;

/**
 * Util methods.
 */
public final class Utils {

  /**
   * Creates a file at the given path.
   *
   * @param fs a filesystem client
   * @param path the file path
   */
  public static void createFile(FileSystem fs, AlluxioURI path) throws Exception {
    try (FileOutStream out = fs.createFile(path, CreateFilePOptions.newBuilder()
        .setBlockSizeBytes(Constants.KB).setRecursive(true).build())) {
      out.write("test".getBytes());
    }
  }

  /**
   * Creates a file at the given path.
   *
   * @param fs a filesystem client
   * @param path the file path
   * @param options create file options
   */
  public static void createFile(FileSystem fs, AlluxioURI path, CreateFilePOptions options)
      throws Exception {
    try (FileOutStream out = fs.createFile(path, options)) {
      out.write("test".getBytes());
    }
  }

  private Utils() {} // Prevent instantiation
}
