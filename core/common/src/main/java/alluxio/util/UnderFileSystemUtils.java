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

package alluxio.util;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility functions for working with {@link alluxio.underfs.UnderFileSystem}.
 *
 */
@ThreadSafe
public final class UnderFileSystemUtils {

  /**
   * Deletes the directory at the given path if it exists.
   *
   * @param ufs instance of {@link UnderFileSystem}
   * @param path path to the directory
   */
  public static void deleteDirIfExists(UnderFileSystem ufs, String path) throws IOException {
    if (ufs.isDirectory(path)
        && !ufs.deleteDirectory(path, DeleteOptions.defaults().setRecursive(true))) {
      throw new IOException("Folder " + path + " already exists but can not be deleted.");
    }
  }

  /**
   * Attempts to create the directory if it does not already exist.
   *
   * @param ufs instance of {@link UnderFileSystem}
   * @param path path to the directory
   */
  public static void mkdirIfNotExists(UnderFileSystem ufs, String path) throws IOException {
    if (!ufs.isDirectory(path)) {
      if (!ufs.mkdirs(path)) {
        throw new IOException("Failed to make folder: " + path);
      }
    }
  }

  /**
   * Creates an empty file.
   *
   * @param ufs instance of {@link UnderFileSystem}
   * @param path path to the file
   */
  public static void touch(UnderFileSystem ufs, String path) throws IOException {
    OutputStream os = ufs.create(path);
    os.close();
  }

  /**
   * Deletes the specified path from the specified under file system if it is a file and exists.
   *
   * @param ufs instance of {@link UnderFileSystem}
   * @param path the path to delete
   */
  public static void deleteFileIfExists(UnderFileSystem ufs, String path) {
    try {
      if (ufs.isFile(path)) {
        ufs.deleteFile(path);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is a Google cloud store implementation
   */
  public static boolean isGcs(UnderFileSystem ufs) {
    return "gcs".equals(ufs.getUnderFSType());
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is a local file system implementation
   */
  public static boolean isLocal(UnderFileSystem ufs) {
    return "local".equals(ufs.getUnderFSType());
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is a Hadoop distributed file system implementation
   */
  public static boolean isHdfs(UnderFileSystem ufs) {
    return "hdfs".equals(ufs.getUnderFSType());
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is an Object storage service implementation
   */
  public static boolean isOss(UnderFileSystem ufs) {
    return "oss".equals(ufs.getUnderFSType());
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is an S3 implementation
   */
  public static boolean isS3(UnderFileSystem ufs) {
    return "s3".equals(ufs.getUnderFSType()) || "s3a".equals(ufs.getUnderFSType());
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is a Swift storage implementation
   */
  public static boolean isSwift(UnderFileSystem ufs) {
    return "swift".equals(ufs.getUnderFSType());
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is a Http implementation
   */
  public static boolean isWeb(UnderFileSystem ufs) {
    return "web".equals(ufs.getUnderFSType());
  }

  /**
   * @param ufs the {@link UnderFileSystem} implementation to check
   * @return true if the implementation is a Cephfs storage implementation
   */
  public static boolean isCephFS(UnderFileSystem ufs) {
    return "cephfs".equals(ufs.getUnderFSType());
  }

  /**
   * @param uri the UFS path
   * @return the bucket or container name of the object storage
   */
  public static String getBucketName(AlluxioURI uri) {
    return uri.getAuthority().toString();
  }

  /**
   * Returns an approximate content hash, using the length and modification time.
   *
   * @param length the content length
   * @param modTime the content last modification time
   * @return the content hash
   */
  public static String approximateContentHash(long length, long modTime) {
    // approximating the content hash with the file length and modtime.
    StringBuilder sb = new StringBuilder();
    sb.append('(');
    sb.append("len:");
    sb.append(length);
    sb.append(", ");
    sb.append("modtime:");
    sb.append(modTime);
    sb.append(')');
    return sb.toString();
  }

  private UnderFileSystemUtils() {} // prevent instantiation
}
